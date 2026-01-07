import logging
from configs.constants import (
    BASE_S3_BUCKET,
    S3_FOLDER_TEMPLATES,
    S3_FOLDER_MAPPING,
    MAPPING_S3_KEY,
    FINAL_IMAGES_FOLDER,
    FINAL_REPORTS_FOLDER,
    TEMP_S3_FOLDER, 
    TEMPLATE_NAME, 
    PLACE_HOLDER, 
    SOURCE_PDF_NAME, 
    FIGURES
)
# from configs.constants import *
from handlers.s3_handler import S3Handler
from processors.mapping_processor import MappingProcessor
# from processors.docx_image_extractor import DocxImageExtractor
from processors.pdf_image_extractor import PDFImageExtractor
from generators.report_generator import ReportGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cleanup_temp_folder(s3_handler: S3Handler, folder_prefix: str, do_cleanup: bool = True):
    """
    Deletes the temporary folder contents in S3 if do_cleanup is True.

    Args:
        s3_handler (S3Handler): S3 handler object.
        folder_prefix (str): Prefix folder in S3 to delete.
        do_cleanup (bool): Whether to delete folder. Default True.
    """
    if do_cleanup:
        try:
            logger.info(f"Cleaning up temporary folder {folder_prefix} in bucket {s3_handler.bucket_name}")
            # s3_handler.delete_folder(folder_prefix)
        except Exception as e:
            logger.error(f"Failed to cleanup temporary folder {folder_prefix}: {e}")

def main():
    s3_handler = S3Handler(BASE_S3_BUCKET)
    mapping_processor = MappingProcessor(s3_handler)
    # extractor = DocxImageExtractor(s3_handler)
    extractor = PDFImageExtractor(s3_handler)
    report_generator = ReportGenerator(s3_handler)

    # Step 1: Download and parse mapping
    try:
        mapping_local_path = mapping_processor.download_mapping_excel()
        mappings = mapping_processor.parse_mapping(mapping_local_path)
        print("\n\nMapping process done.")
    except Exception as e:
        logger.error(f"Aborting due to mapping error: {e}")
        return

    # Step 2: Extract images based on mapping and upload to final images folder inside temp folder
    all_uploaded_images = []
    for mapping in mappings:
        # TODO: Modify to have the fully qualified file path
        # source_docx_key = f"{S3_FOLDER_TEMPLATES}/{mapping['source_docx']}"

        # print("type(mapping)")
        # print(type(mapping))
        logger.info(f"\nmapping:\t {mapping}")
        source_docx_key = f"{S3_FOLDER_TEMPLATES}/{mapping[SOURCE_PDF_NAME]}"
        # doc_id = mapping[TEMPLATE_NAME]
        # template_key = f"{S3_FOLDER_TEMPLATES}/{mapping[TEMPLATE_NAME]}"
        template_key = mapping[TEMPLATE_NAME]
        figures = mapping[FIGURES]

        # Destination folder is temp folder + final images subfolder
        s3_image_folder = f"{S3_FOLDER_MAPPING}/{TEMP_S3_FOLDER}/{FINAL_IMAGES_FOLDER}"
        # s3_image_folder = f"{S3_FOLDER_MAPPING}/{FINAL_IMAGES_FOLDER}"
        # s3_image_folder = f"{TEMP_S3_FOLDER}/{FINAL_IMAGES_FOLDER}"

        print(f"\n\ns3_image_folder: {s3_image_folder}")
        print(f"S3_FOLDER_MAPPING: {S3_FOLDER_MAPPING}")
        print(f"TEMP_S3_FOLDER: {TEMP_S3_FOLDER}")
        print(f"FINAL_IMAGES_FOLDER: {FINAL_IMAGES_FOLDER}")

        try:
            uploaded_images = extractor.extract_images_and_upload(
                source_docx_key,
                figures,
                s3_image_folder,
                template_key
            )
            logger.info(f"\nfigures upload: {figures}")
            all_uploaded_images.extend(uploaded_images)
        except Exception as e:
            logger.error(f"Error extracting images for doc {template_key}: {e}")

    # Step 3: Generate report by inserting images into template
    # template_key = f"{S3_FOLDER_TEMPLATES}/{DEFAULT_TEMPLATE}"
    # template_key = mapping[TEMPLATE_NAME]
    output_report_name = "final_report.docx"
    output_s3_folder = f"{S3_FOLDER_MAPPING}/{TEMP_S3_FOLDER}/{FINAL_REPORTS_FOLDER}"

    try:
        report_generator.generate_report(
            f"{S3_FOLDER_TEMPLATES}/{mapping[TEMPLATE_NAME]}",
            mappings,
            s3_image_folder,
            output_s3_folder,
            output_report_name
        )
        print("\nreports generated")
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        return

    # Step 4: Cleanup temporary files in S3 - toggle here
    cleanup_temp_folder(s3_handler, TEMP_S3_FOLDER, do_cleanup=True)
    print("\nreports generated")
    logger.info("Processing completed successfully.")

if __name__ == "__main__":
    main()
