import pandas as pd
import re
import camelot
from dagster import asset, Output, MetadataValue, AssetExecutionContext, get_dagster_logger
from PyPDF2 import PdfReader
from concurrent.futures import ThreadPoolExecutor

PAGE_CHUNK_SIZE = 10

FINAL_COLUMNS = ["key", "Total", "White", "Black", "Hispanic", "Native American", "Asian", "Other", "department", "table name"]

def process_chunk(args):
    logger = get_dagster_logger()
    pdf_path, page_range = args

    logger.info(f"Starting PDF processing for file: {pdf_path} on pages: {page_range}")
    try:
        tables = camelot.read_pdf(pdf_path, pages=page_range, flavor='stream')
        logger.info(f"PDF read complete. Detected {len(tables)} table(s) on pages {page_range}")
    except Exception as e:
        logger.error(f"Error reading PDF {pdf_path} on pages {page_range}: {e}")
        return []

    chunk_results = []

    for table_idx, table in enumerate(tables):
        logger.info(f"Processing table #{table_idx} from pages {page_range}")
        df = table.df.copy()
        logger.info(f"Initial DataFrame shape for table #{table_idx}: {df.shape}")
        logger.debug(f"INITIAL TABLE #{table_idx}:\n{df.to_string()}")

        # Drop entirely empty columns.
        df = df.dropna(axis=1, how='all')
        logger.debug(f"DataFrame shape after dropping empty columns: {df.shape}")

        # Look for the metadata row (e.g., "Table 1: Rates by Race for Adair County Sheriﬀ’s Dept")
        metadata_row_idx = None
        metadata_line = None
        for i, row in df.iterrows():
            line = " ".join(row.dropna())
            if re.search(r"Table\s*\d+:", line):
                metadata_row_idx = i
                metadata_line = line
                logger.debug(f"Found metadata at row {i}: {metadata_line}")
                break

        if metadata_row_idx is None or not metadata_line:
            logger.error(f"Missing metadata in table #{table_idx} from pages {page_range}, skipping.")
            continue

        # Extract metadata using regex.
        m = re.search(r"Table\s*(\d+):\s*(.*?)\s*for\s*(.+)", metadata_line)
        if m:
            table_number = m.group(1)
            table_name = m.group(2).strip()
            dept_name = m.group(3).strip()
            logger.info(f"Extracted metadata for table #{table_idx}: Table Name='{table_name}', Department='{dept_name}'")
        else:
            logger.error(f"Could not parse metadata from line: '{metadata_line}' in table #{table_idx}, skipping.")
            continue

        # Drop the metadata row and header row.
        # Assume the metadata row is at metadata_row_idx and the next row is a header.
        df = df.iloc[metadata_row_idx+2:].reset_index(drop=True)
        logger.debug(f"DataFrame shape after removing metadata and header rows: {df.shape}")

        # If the first cell in the first row is empty, shift columns left.
        if pd.isna(df.iloc[0, 0]) or str(df.iloc[0, 0]).strip() == "":
            logger.info(f"First cell of table #{table_idx} is empty. Dropping the first column to realign data.")
            df = df.iloc[:, 1:]
            logger.debug(f"DataFrame shape after shifting columns: {df.shape}")

        # Now, select only the first 8 columns for the key and race data.
        if df.shape[1] < 8:
            logger.error(f"Unexpected column count in table #{table_idx}: {df.shape[1]}, skipping.")
            continue

        try:
            df = df.iloc[:, :8]  # get the 8 columns needed for final output
            df.columns = FINAL_COLUMNS[:8]  # assign column names for the key and race breakdown
        except Exception as e:
            logger.error(f"Error reassigning columns for table #{table_idx}: {e}")
            continue

        # Append metadata columns
        df["department"] = dept_name
        df["table name"] = table_name

        logger.info(f"Final structured DataFrame for table #{table_idx}:\n{df.to_string()}")
        chunk_results.append(df)

    return chunk_results

@asset(
    description="Extracts data from PDF files and processes it into a DataFrame.",
    required_resource_keys={"data_dir_report_pdfs", "data_dir_processed"},
)
def extract_pdf_data(context: AssetExecutionContext) -> pd.DataFrame:
    """Extracts relevant tables from the PDF using Camelot."""

    pdf_file = context.resources.data_dir_report_pdfs.get_path() / "VSRreport2023.pdf"
    pdf_reader = PdfReader(pdf_file)

    total_pages = len(pdf_reader.pages)
    context.log.info(f"Total pages in PDF: {total_pages}")

    total_pages = 100

    chunks = [
        f"{i}-{min(i + PAGE_CHUNK_SIZE - 1, total_pages)}"
        for i in range(1, total_pages + 1, PAGE_CHUNK_SIZE)
    ]
    context.log.info(f"Processing {len(chunks)} chunks: {chunks}")

    args = [(pdf_file, chunk) for chunk in chunks]

    all_results = []
    with ThreadPoolExecutor() as executor:
        results = executor.map(process_chunk, args)
        for result in results:
            all_results.extend(result)

    if not all_results:
        context.log.error("No tables extracted from PDF. Check extraction logic.")
        raise ValueError("No tables were extracted from the PDF.")

    combined_df = pd.concat(all_results, ignore_index=True)
    combined_csv_path = context.resources.data_dir_processed.get_path() / "combined_output.csv"
    combined_df.to_csv(combined_csv_path, index=False)
    context.log.info(f"Extracted {len(combined_df)} rows across {len(all_results)} tables.")
    return combined_df
