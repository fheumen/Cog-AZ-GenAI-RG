INDEX_NAME = "azreportgenindex" ### Pinecone Index Name
INTPUTS_PATH = "/dbfs/mnt/FileStore/inputs"  ### intput directory, where all intputs  files are save
OUTPUTS_PATH = "/dbfs/mnt/FileStore/outputs"  ### output directory, where all output files are save

section_names = ["summary and conclusion", "Batches Reviewed", "Reprocessed",
                 "Product Reviews from", "Starting and Packaging Materials",
                 "Analytical Data", "Changes", "Stability data", "Deviations", "Quality Events", "Complaints (Product Quality)",
                 "Recalls, Stock recoveries, Field Alerts", "Returned and salvaged goods",
                 "CONTRACTUAL AGREEMENTS / ARRANGEMENTS", "Qualification status", "Other"]

section_pattern = [r"summary and conclusion", r"Batches Reviewed", r"Reprocessed",
                 r"Product Reviews from", r"Starting and Packaging Materials",
                 r"Analytical Data", r"Changes", r"Stability data", r"Deviations", r"Quality Events", r"Complaints (Product Quality)",
                 r"Recalls, Stock recoveries, Field Alerts", r"Returned and salvaged goods",
                 r"CONTRACTUAL AGREEMENTS / ARRANGEMENTS", r"Qualification status", r"Other"]

site_names = ["fmc", "sbc", "speke"]

product_names = ["Fasenra"]
#product_names = ["Lumoxity", "Enhertu", "Vaxzevria", "Beyfortus", "IMJUDO", "IMFINZI", "Synagis", "Saphnelo", "Fasenra", "Tezspire"]

reporting_periods = ["14Nov2022_13Nov2013"]
