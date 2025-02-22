import argparse
import os
import subprocess
from multiprocessing import Pool

def find_pdf_files(input_dir):
    """Recursively find all PDF files in the input directory."""
    pdf_files = []
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))
    return pdf_files

def get_output_path(input_file, input_dir, output_dir):
    """Generate output path preserving directory structure."""
    input_abs = os.path.abspath(input_dir)
    rel_path = os.path.relpath(input_file, input_abs)
    return os.path.join(os.path.abspath(output_dir), rel_path)

def process_file(input_path, output_path):
    """Process a single PDF file with Ghostscript."""
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)
    
    # Ghostscript command configuration
    command = [
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
        f'-sOutputFile={output_path}',
        input_path
    ]
    
    error_msg = None
    original_size = None
    compressed_size = None
    ratio = None
    
    # Get original file size
    try:
        original_size = os.path.getsize(input_path)
    except Exception as e:
        error_msg = f"Error getting original size: {str(e)}"
        return (input_path, output_path, original_size, compressed_size, ratio, error_msg)
    
    # Run Ghostscript command
    try:
        result = subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    except subprocess.CalledProcessError as e:
        error_msg = f"Ghostscript error ({e.returncode}): {e.stderr.decode('utf-8')}"
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
    else:
        # Get compressed file size
        try:
            compressed_size = os.path.getsize(output_path)
            if original_size > 0:
                ratio = compressed_size / original_size
            else:
                ratio = float('inf')
        except Exception as e:
            error_msg = f"Error getting compressed size: {str(e)}"
    
    return (input_path, output_path, original_size, compressed_size, ratio, error_msg)

def main():
    parser = argparse.ArgumentParser(description='PDF Compressor using Ghostscript')
    parser.add_argument('--input', required=True, help='Input directory containing PDF files')
    parser.add_argument('--output', required=True, help='Output directory for compressed PDFs')
    parser.add_argument('--num_workers', type=int, required=True, 
                       help='Number of parallel workers for processing')
    args = parser.parse_args()

    # Find all PDF files and prepare tasks
    pdf_files = find_pdf_files(args.input)
    tasks = [(pdf_file, get_output_path(pdf_file, args.input, args.output)) 
             for pdf_file in pdf_files]

    # Process files in parallel
    with Pool(args.num_workers) as pool:
        results = pool.starmap(process_file, tasks)

    # Print results in CSV format
    print("input_path,output_path,original_size,compressed_size,compression_ratio,error")
    for result in results:
        input_path, output_path, orig, comp, ratio, err = result
        
        # Format ratio for output
        ratio_str = ""
        if ratio is not None:
            ratio_str = f"{ratio:.4f}" if ratio != float('inf') else "INF"
        
        # Format fields with proper quoting
        fields = [
            f'"{input_path}"',
            f'"{output_path}"',
            str(orig) if orig is not None else "",
            str(comp) if comp is not None else "",
            ratio_str,
            f'"{err}"' if err is not None else ""
        ]
        
        print(",".join(fields))

if __name__ == '__main__':
    main()
