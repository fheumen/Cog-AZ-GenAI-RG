INDEX_NAME = "azreportgenindex" ### Pinecone Index Name

BUCKET_NAME = "s3-az-reportgen-bucket"
INTPUTS_PATH = "inputs/"  ### intput directory, where all intputs  files are save
UPLOAD_TMP_PATH = "tmp/"  ### intput directory, where all files uploaded by the user are temporarily saved
OUTPUTS_PATH = "outputs/"  ### output directory, where all output files are save

bucket_name =  f"{BUCKET_NAME}"
input_folder = f"{INTPUTS_PATH}"  ### intput directory, where all intputs  files are save
output_folder = f"{OUTPUTS_PATH}"  ### output directory, where all output files are save
upload_folder = f"{UPLOAD_TMP_PATH}"

section_names = ["summary and conclusion", "Batches Reviewed", "Reprocessed",
                 "Product Reviews from", "Starting and Packaging Materials",
                 "Analytical Data", "Changes", "Stability data", "Deviations", "Quality Events", "Complaints (Product Quality)",
                 "Recalls, Stock recoveries, Field Alert", "Returned and salvaged goods",
                 "CONTRACTUAL AGREEMENTS / ARRANGEMENTS", "Qualification status", "Other"]

section_pattern = [r"summary and conclusion", r"Batches Reviewed", r"Reprocessed",
                 r"Product Reviews from", r"Starting and Packaging Materials",
                 r"Analytical Data", r"Changes", r"Stability data", r"Deviations", r"Quality Events", r"Complaints (Product Quality)",
                 r"Recalls, Stock recoveries, Field Alerts", r"Returned and salvaged goods",
                 r"CONTRACTUAL AGREEMENTS / ARRANGEMENTS", r"Qualification status", r"Other"]

site_names = ["fmc", "sbc", "speke"]

#product_names = ["Fasenra"]
product_names = ["Lumoxity", "Enhertu", "Vaxzevria", "Beyfortus", "IMJUDO", "IMFINZI", "Synagis", "Saphnelo", "Fasenra", "Tezspire"]

reporting_periods = ["14Nov2022_13Nov2013"]

# Regular expression to match the date range (e.g., "14 Nov 2022 – 13 Nov 2023" or "14 Nov 2022 to 13 Nov 2023")
date_pattern = r'(\d{1,2}\s\w+\s\d{4})\s[–-]\s(\d{1,2}\s\w+\s\d{4})|(\d{1,2}\s\w+\s\d{4})\sto\s(\d{1,2}\s\w+\s\d{4})|(\w+\s\d{1,2},\s\d{4})\sthrough\s(\w+\s\d{1,2},\s\d{4})'

# Regular expression to match header and the footer, in order to avoid extracting them
header_pattern = r'REP-\d{7}\sv\d{1}.\d{1}\sStatus: Approved Approved Date:\s\d{1,2}\s\w+\s\d{4}\sPage\s\d{1,3}\sof\s\d{1,3}'
footer_pattern = r'Check this is the latest version of the document before use.'