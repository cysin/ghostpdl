import argparse
import datetime
import logging
import multiprocessing as mp
import random
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Dict, List, Tuple

import fitz  # PyMuPDF

def setup_logging(logs_dir: Path, zip_name: str) -> logging.Logger:
    """Setup logging for a specific ZIP file."""
    log_file = logs_dir / f"{zip_name}.log"
    logger = logging.getLogger(zip_name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger

def compress_pdf_in_memory(pdf_data: bytes) -> bytes:
    """Compress PDF using Ghostscript with specified settings."""
    try:
        gs_command = [
            '/opt/ghostscript/bin/gs',
            '-sDEVICE=pdfwrite',
            '-dCompatibilityLevel=1.4',
            '-dPDFSETTINGS=/screen',
            '-dColorImageFilter=/DCTEncode',
            '-dGrayImageFilter=/DCTEncode',
            '-dColorImageDownsampleType=/Bicubic',
            '-dGrayImageDownsampleType=/Bicubic',
            '-dMonoImageDownsampleType=/Subsample',
            '-dColorImageResolution=108',
            '-dGrayImageResolution=108',
            '-dMonoImageResolution=108',
            '-dNOPAUSE',
            '-dQUIET',
            '-dBATCH',
            '-sOutputFile=-',
            '-'
        ]
        
        process = subprocess.Popen(
            gs_command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        compressed_data, stderr = process.communicate(input=pdf_data)
        
        if process.returncode != 0:
            raise Exception(f"Ghostscript error: {stderr.decode()}")
            
        return compressed_data
    except Exception as e:
        raise Exception(f"Compression failed: {str(e)}")

def is_scanned_pdf(pdf_data: bytes, num_pages_to_check: int = 3) -> bool:
    """Check if PDF is a scanned document containing only images."""
    try:
        with fitz.open(stream=pdf_data, filetype="pdf") as doc:
            total_pages = len(doc)
            if total_pages == 0:
                return False
            
            num_pages_to_check = min(num_pages_to_check, total_pages)
            sample_pages = random.sample(range(total_pages), num_pages_to_check)
            
            for page_num in sample_pages:
                page = doc.load_page(page_num)
                if page.get_text().strip():
                    return False
                if not page.get_images(full=True):
                    return False
            return True
    except Exception:
        return False

def process_file(args: Tuple[Path, Path, Path, Path, Path]) -> Dict:
    """Process a single file from a ZIP archive."""
    zip_path, file_path, compressed_dir, scanned_dir, failure_dir = args
    zip_name = zip_path.stem
    result = {
        'zip_name': zip_name,
        'file_path': str(file_path),
        'status': 'failed',
        'original_size': 0,
        'compressed_size': 0,
        'compression_ratio': 0,
        'error': ''
    }

    try:
        # Read file from ZIP
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_data = zip_ref.read(str(file_path))
            result['original_size'] = len(file_data) / 1024  # KB

        # Validate PDF
        try:
            with fitz.open(stream=file_data, filetype="pdf") as doc:
                pass  # Just validate PDF
        except Exception as e:
            raise Exception(f"Invalid PDF: {str(e)}")

        # Check for scanned PDF
        if is_scanned_pdf(file_data):
            output_path = scanned_dir / zip_name / file_path
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(file_data)
            result.update({
                'status': 'success (scanned)',
                'compressed_size': result['original_size']
            })
            return result

        # Attempt compression
        try:
            compressed_data = compress_pdf_in_memory(file_data)
            compressed_size = len(compressed_data) / 1024  # KB

            if compressed_size < result['original_size']:
                output_path = compressed_dir / zip_name / file_path
                save_data = compressed_data
                compression_status = 'compressed'
            else:
                output_path = compressed_dir / zip_name / file_path
                save_data = file_data
                compression_status = 'uncompressed (larger size)'

            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(save_data)
            
            result.update({
                'status': f'success ({compression_status})',
                'compressed_size': len(save_data) / 1024,
                'compression_ratio': ((result['original_size'] - (len(save_data)/1024)) 
                                    / result['original_size'] * 100)
            })

        except Exception as e:
            # Save to failure directory with original structure
            output_path = failure_dir / zip_name / file_path
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(file_data)
            raise Exception(f"Compression failed: {str(e)}")

    except Exception as e:
        result.update({
            'status': f'failed: {str(e)}',
            'error': str(e),
            'compressed_size': result['original_size']
        })
        # Ensure failure directory structure
        output_path = failure_dir / zip_name / file_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(file_data if 'file_data' in locals() else b'')

    return result

def main():
    parser = argparse.ArgumentParser(description='Process PDF files in ZIP archives')
    parser.add_argument('input_dir', help='Input directory containing ZIP files')
    parser.add_argument('output_dir', help='Base output directory')
    parser.add_argument('--num_workers', type=int, default=mp.cpu_count(),
                       help='Number of worker processes')

    args = parser.parse_args()

    # Create directory structure
    output_dir = Path(args.output_dir)
    compressed_dir = output_dir / 'compressed'
    scanned_dir = output_dir / 'scanned_images'
    failure_dir = output_dir / 'failure'
    logs_dir = output_dir / 'logs'

    for d in [compressed_dir, scanned_dir, failure_dir, logs_dir]:
        d.mkdir(parents=True, exist_ok=True)

    # Collect all files in ZIP archives
    task_args = []
    for zip_path in Path(args.input_dir).glob('**/*.zip'):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                # Skip directories
                if file_info.is_dir():
                    continue
                file_path = Path(file_info.filename)
                task_args.append((
                    zip_path,
                    file_path,
                    compressed_dir,
                    scanned_dir,
                    failure_dir
                ))

    # Process files in parallel
    with mp.Pool(processes=args.num_workers) as pool:
        for result in pool.imap_unordered(process_file, task_args):
            logger = setup_logging(logs_dir, result['zip_name'])
            log_msg = (
                f"{result['file_path']} | "
                f"Status: {result['status']} | "
                f"Original: {result['original_size']:.2f}KB | "
                f"Compressed: {result['compressed_size']:.2f}KB | "
                f"Ratio: {result['compression_ratio']:.2f}%"
            )
            if result['error']:
                log_msg += f" | Error: {result['error']}"
            
            if 'failed' in result['status']:
                logger.error(log_msg)
            else:
                logger.info(log_msg)

if __name__ == "__main__":
    main()
