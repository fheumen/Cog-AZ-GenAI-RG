from pydantic import BaseModel
from typing import Optional, List, Dict, Literal
from fastapi import UploadFile, Form

class QueryRequest(BaseModel):
    input_text: str
    knowledge_base: str
    session_id: str

# Define the model for Q&A response
class QnaAnswer(BaseModel):
    answer: str 
    filename: str
    filepath: str
    knowledgeId: str
    sessionId: str

# Define the model for additional instructions
class AnswerRequest(BaseModel):
    additional_instructions: Optional[str] = None    

class User(BaseModel):
    id: str
    sessionId: str
    language: str
    platform: str

class Query(BaseModel):
    text: str
    knowledgeType: Optional[str] = "PQR enqueries"
    transactionCount: int 

class RequestQuery(BaseModel):
    apiKey: str
    user: User
    query: Query

class Citation(BaseModel):
    fileName: Optional[str] = None
    location: Optional[str] = None
    templateName: Optional[str] = None
    productName: Optional[str] = None
    siteName: Optional[str] = None
    reportingPeriod: Optional[str] = None
    created_by: Optional[str] = None
    created_at: Optional[int] = None
    report_id: Optional[str] = None
    #filePath: str
    pageNumber: Optional[int] = None
    sectionName: Optional[str] = None

class QuickReply(BaseModel):
    text: str
    payload: str
    
class FeedbackDisplayOptions(BaseModel):
    thumbsUp: Optional[str] = "N"  
    thumbsDown: Optional[str] = "N"  
    feedbackText: Optional[str] = "N"  

class Feedback(BaseModel):
    feedbackDisplayOptions: FeedbackDisplayOptions

class Result(BaseModel):
    messageId: str
    answer: str
    feedback: Feedback
    transactionCount: Optional[int] = 0
    citations: Optional[List[Citation]] = None  # Optional field
    quickReplies: Optional[List[QuickReply]] = None  # Optional field    

class QueryResponse(BaseModel):
    status: str
    sessionId: str
    userQuery: str
    result: Result
    
class ChatMetadata(BaseModel):
    FileName: Optional[str] = None
    FileLocation: Optional[str] = None
    FlowName: Optional[str] = None
    KbType: Optional[str] = None
    Department: Optional[str] = None

class ChatInteraction(BaseModel):
    #apiKey: Optional[str] = None
    UserId: Optional[str] = None
    SessionId: Optional[str] = None    
    MessageId: Optional[str] = None
    UserMessage: Optional[str] = None
    BotResponse: Optional[str] = None    
    IsFeedbackPositive: Optional[bool] = None
    FeedbackComment: Optional[str] = None
    Timestamp: Optional[str] = None
    SessionStatus: Optional[str] = None
    ChatMetadata: ChatMetadata
    
class ChatHistorySearchRequest(BaseModel):
    #apiKey: Optional[str] = None
    userId: Optional[str] = None
    keyword: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    session_id: Optional[str] = None
    sort_order: Optional[Literal["asc", "desc"]] = "asc"

class FeedbackRequest(BaseModel):
    #apiKey: Optional[str] = None
    userId: Optional[str] = None
    sessionId: Optional[str] = None
    messageId: Optional[str] = None
    isFeedbackPositive: bool
    feedbackComment: Optional[str] = None

class IngestResult(BaseModel):
    fileName: Optional[str] = None
    status: Optional[str] = None
    comment: Optional[str] = None

class IngestResponse(BaseModel):
    sessionId: str
    userId: str
    pqr_param_json_filename: Optional[str] = None
    result: Optional[List[IngestResult]] = None  # Optional field

class IsprTrackingSubSection(BaseModel): 
    SectionName: str
    SectionText: str
    SectionTables: Optional[List[str]] = None
    SectionImages: Optional[List[str]] = None
    
class IsprTrackingSection(BaseModel): 
    SectionName: str
    SectionText: Optional[str] = None
    IsFeedbackPositive: Optional[bool] = None
    FeedbackComment:  Optional[str] = None
    SectionTables: Optional[List[str]] = None
    SectionImages: Optional[List[str]] = None
    Subsection: Optional[List[IsprTrackingSubSection]] = None
    completionStatus: int
    
class IsprTrackingCompletion(BaseModel): 
    SessionID: str
    Timestamp: str
    IsprFilename: str
    Ispr_Editor_Status: Optional[bool] = None
    pqr_param_json_filename: str
    ProductName: str
    ProductName_User: str
    ReportingPeriod: str
    PqrFilename:  Optional[List[str]] = None 
    SiteName:  Optional[List[str]] = None 
    firstDateOfCreation: str
    createdBy: str
    DateofEdition: Optional[str] = None
    editBy: Optional[str] = None
    template_name: Optional[str] = "ISPR"
    html_string: Optional[str] = "<html></html>"
    DetailCompletionStatus: Optional[List[IsprTrackingSection]]
    OverallCompletionStatus: int
    
    
class IsprTrackingWelcomePageAllProduct(BaseModel):
    user: User
    ispr_status_main: Optional[List[IsprTrackingCompletion]] = None 

    
class IsprFile(BaseModel): 
    apiKey: Optional[str] = None
    IsprFilename: str
    pqr_param_json_filename: Optional[str] = None
    
    
class IsprSelectForEditionOutput(BaseModel): 
    user: User
    ispr_select_edit: IsprTrackingCompletion


class IsprUpdatingPage(BaseModel):
    user: User
    ispr_actual_status: IsprTrackingCompletion  
    ActualCompletionValues: Optional[List[int]] = None ############ List of 16 Integer Value, between 0 and 100
    
    
class IsprTrackingProduct(BaseModel): 
    user: User
    ProductName: str
    ProductName_User: str

class ProductVersion(BaseModel): 
    ProductName_User: str
    
class User_context(BaseModel):
    id: str
    sessionId: str
    language: str
    platform: str
    # pqr_param_json_filename: Optional[str] = None ############## Mandatory for ISPR
    template_name: Optional[str] = "ISPR"
    
class PqrTrackingStatus(BaseModel): 
    UserId: str
    Timestamp: str
    SessionID: str
    IsprFilename: Optional[str] = None
    ProductName: str
    ReportingPeriod: str
    SiteName:  Optional[List[str]] = None
    Versions:  Optional[List[str]] = None
    PqrFilename:  Optional[List[str]] = None 
    pqr_status: Optional[str] = None

##################################### For Report Management
class ReportTrackingSection(BaseModel): 
    section_name: str
    section_text: Optional[str] = None
    is_feedback_positive: Optional[bool] = None
    feedback_comment:  Optional[str] = None
    section_tables: Optional[List[str]] = None
    section_images: Optional[List[str]] = None
    subsection: Optional[List[IsprTrackingSubSection]] = None
    completion: int


class ReportTrackingCompletion(BaseModel):     
    report_id: str
    created_by: str
    name: str
    session_id: str
    created_at: str
    file_version: int
    report_file_path: str
    template_name: Optional[str] = "ISPR"
    template_fullname: Optional[str] = "ISPR Report"
    product_name: str  ############ Or Molecule Name 
    pqr_param_json_filename: Optional[str] = None
    reporting_period: Optional[str] = None
    edit_status: str
    completion_detail_section: Optional[List[ReportTrackingSection]]
    completion: int
    updated_at: Optional[str] = None
    edited_by: Optional[str] = None
    locked: Optional[bool] = None
    source_file_names: List[str]
    site_names:  Optional[List[str]] = None
    report_title: Optional[str] = None
    html_content: Optional[str] = "<html></html>"
    
    
class ReportTrackingWelcomePageAllProduct(BaseModel):
    user: User
    report_status_main: Optional[List[ReportTrackingCompletion]] = None 
    
class WelcomeFilter(BaseModel):
    user: User
    pr_ids:  Optional[List[str]] = None
    statuses:  Optional[List[str]] = ["Ready For review", "In Progress", "Completed"]
    # sort_by: Optional[str] = "created_at"
    sort_order: Optional[str] = "desc"
    
    
class QnAInputs(BaseModel):
    apiKey: Optional[str] = None
    userId: Optional[str] = None
    sessionId: Optional[str] = None 
    template_name: Optional[str] = None
    product_name: Optional[List[str]] = None 
    created_by: Optional[List[str]] = None 
    # created_at: Optional[str] = None
    created_start_date: Optional[str] = None
    created_end_date: Optional[str] = None
    reporting_period: Optional[List[str]] = None 
    section_name: Optional[List[str]] = None        
    language: Optional[str] = None
    platform: Optional[str] = None
    queryText: Optional[str] = None 
    transactionCount: Optional[int] = None    
################################### For Select Edit
class ReportSelectForEditionOutput(BaseModel): 
    user: User
    report_select_edit: ReportTrackingCompletion
    

################################### For Queue Management
class ReportQueue(BaseModel):
    report_id: Optional[str]=None
    file_version:  Optional[int]=None
    created_by: str
    emailId: str
    name: str
    session_id: str
    created_at:  Optional[str]=None
    updated_at:  Optional[str]=None
    status_in_queue: Optional[str]=None  #queued | processing | completed | failed | deleted
    # template_id discuss with venkat
    upload_folder: str
    pqr_param_json_filename: Optional[str] = None
    product_name : Optional[str]=None
    reporting_period : Optional[str]=None
    validation_status:  Optional[str]=None  #success | failed 
    completion_percentage: Optional[int] = 0
    source_file_names: List[str]
    source_file_types: List[str]
    list_versions: List[str]
    template_fullname: str
    template_name: str
    
# class ReportTrackingCompletion(BaseModel):     
#     report_id: str
#     created_by: str
#     session_id: str
#     created_at: str
#     file_version: str
#     report_file_path: str
#     # template_name: Optional[str] = "ISPR"
#     template_id: str
#     product_name: str  ############ Or Molecule Name 
#     pqr_param_json_filename: Optional[str] = None
#     reporting_period: Optional[str] = None
#     edit_status: str
#     completion_detail_section: Optional[List[ReportTrackingSection]]
#     completion: int
#     updated_at: Optional[str] = None
#     edited_by: Optional[str] = None
#     locked: Optional[bool] = None
#     source_files_names: str
#     site_names:  Optional[List[str]] = None
#     html_content: Optional[str] = "<html></html>"
    
class ReportRequestCreation(BaseModel): #### User would like to insert a rrequest in the queues
    # user: User
    files: list[UploadFile]
    userId: Optional[str] = Form(None)
    sessionId: Optional[str] = Form(None)
    template_fullname: Optional[str] = Form(None)
    # request_object: ReportQueue
    # checksum_source_file_names: Optional[List[str]]
    action: Optional[str] = Form(None)
    
class ReportFile(BaseModel): 
    report_id: str
    file_version: int
    report_file_path: str
    new_title: Optional[str] = None
    # pqr_param_json_filename: Optional[str] = None
    
class ReportFileRename(BaseModel):
    userId: str
    report_id: str
    file_version: int
    report_file_path: str
    new_title: Optional[str] = None
    # pqr_param_json_filename: Optional[str] = None
    
class UserTemplate(BaseModel):
    user: User
    template_fullname: Optional[str] = "ISPR Editor"
    
class TemplateClass(BaseModel):
    template_fullname: str 
    
class FileNameTypePair(BaseModel):
    file_name: str
    file_type: str

class UserOut(BaseModel):
    pr_id: str
    name: str
    email_id: Optional[str] = None

class Section(BaseModel):
    title: str
    desc: str

class TemplateMaster(BaseModel):
    template_id: str
    created_at: Optional[str] = None
    file_version: Optional[int] = None
    approval_status: Optional[str] = None
    created_by: Optional[str] = None
    name: Optional[str] = None
    doc_types: Optional[List[str]] = None          # changed from List[Dict[str, str]]
    doc_types_full: Optional[List[str]] = None     # changed from List[Dict[str, str]]
    is_active: Optional[bool] = True
    locked: Optional[bool] = None
    kb_name: Optional[str] = None
    product_names: Optional[List[str]] = None      # changed from List[Dict[str, str]]
    s3_excel_path: Optional[str] = None
    s3_path: Optional[str] = None
    section_names_desc: Optional[List[Section]] = []
    section_names: Optional[List[str]] = None  # Only titles stored
    sharepoint_link: Optional[str] = None
    # section_descs: Optional[List[str]] = None
    template_fullname: Optional[str] = None
    template_name: Optional[str] = None
    updated_at: Optional[str] = None
    updated_by: Optional[str] = None

class TemplateDashboardResponse(BaseModel):
    user: User
    template_status_main: Optional[List[TemplateMaster]] = None

class TemplateCreateRequest(BaseModel):
    user_id: str
    section_names: Optional[List[Section]] = []
    document_name: Optional[str] = "test"

class TemplateFile(BaseModel):
    template_id: str
    s3_path: str

class TemplateFilter(BaseModel):
    user: User
    pr_ids:  Optional[List[str]] = None
    statuses:  Optional[List[str]] = ["In progress", "Approved", "In approval"]
    # sort_by: Optional[str] = "created_at"
    sort_order: Optional[str] = "desc"
