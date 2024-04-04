import datetime
import glob
import os
from enum import Enum
from xml.dom import minidom
from xml.etree import ElementTree as ET

import geopandas as gpd
import rasterio


class ConfigKeys(Enum):
    """ JSON Configuration file keys """
    LOGGING = "logging"
    LOG_FILE_PATH = "log_file_path"
    ALERT = "alert"
    SENDER_EMAIL = "sender_email"
    RECIPIENT_EMAIL = "recipient_email"
    CONTAINERS = "containers"
    AEROSOL = "aerosol"
    CLOUD_COVER = "cloud_cover"
    LST = "lst"
    PRECIPITATION_AMOUNT = "precipitation_amount"
    SNOW_COVER = "snow_cover"
    SOLAR_RADIATION = "solar_radiation"
    SST = "sst"
    CONTAINER_PATH = "container_path"
    SERVICE_NAME = "service_name"
    PERIOD_MINUTES = "period_minutes"
    YEAR = "year"
    DAY = "day"
    MONTH = "month"
    HOUR_ST = "hour_st"
    MIN_ST = "min_st"
    TIMESTAMPS = "timestamps"
    SHAPEFILE_PATH = "shapefile_path"
    CRS_PATH = "crs_path"
    TIFF_FOLDER = "tiff_folder"
    MONGO = "mongo"
    MONGO_HOST = "mongo_host"
    MONGO_PORT = "mongo_port"
    DATABASE = "database"
    COLLECTION = "collection"
    SFTP = "sftp"
    USER = "user"
    PWD = "pwd"
    HOST = "host"
    REMOTE_DIR = "remote_dir"
    S3 = "s3"
    AWS_ACCESS_KEY = "aws_access_key"
    AWS_SECRET_KEY = "aws_secret_key"
    REGION = "region"
    BUCKET_NAME = "bucket_name"
    LOCAL_CONFIGS = "local_configs"
    LOCAL_BASE_PATH = "local_base_path"
    REMOTE_BASE_PATH = "remote_base_path"
    SERVICES = "services"
    DAILY_DATE = "daily_date"
    HOURLY_DATE = "hourly_date"
    REFERENCE_IMAGE_PATH = "reference_image_path"
    SLEEP_MINUTES = "sleep_minutes"
    HOURS_BACK = "hours_back"
    SVC = "svc"
    PRODUCT_ID = "product_id"
    XML_FOLDER = "xml_folder" 
    XML_PARENT = "xml_parent"
    XML_CONFIGS = "xml_configs"
    TITLE_TEXT = "title_text"
    ABSTRACT_STRING = "abstract_string"
    SERVICE_FREQUENCY = "service_frequency"
    MAINTAINANCE_DELAY = "maintainance_delay"
    ANCHOR_1_TEXT = "anchor_1_text"
    RESOLUTION = "resolution"
    UOM = "uom"
    ACCURACY = "accuracy"
    STATEMENT_TEXT = "statement_text"
    POSITIONAL_ACCURACY = "positional_accuracy"
    THEMATIC_ACCURACY = "thematic_accuracy"
    REFERENCE_TIME = "reference_time"
    PRODUCT_URL = "product_url"
    MSG = "msg"
    MSG_INPUT_ID = "msg_input_id"
    FILENAME = "filename"
    DATE_START = "date_start"
    DATE_END = "date_end"
    INPUT_FILES_LIST = "input_files_list"
    GCOM_C_INPUT_ID = "gcom_c_input_id"
    CAMS_INPUT_ID = "cams_input_id"
    S3_INPUT_ID = "s3_input_id"
    CAMS = "cams"
    GCOM_C = "gcom_c"
    S3_RESOLUTION = "s3_resolution"
    S3_UOM = "s3_uom"
    GCOM_C_RESOLUTION = "gcom_c_resolution"
    GCOM_C_UOM = "gcom_c_uom"
    CAMS_RESOLUTION = "cams_resolution"
    CAMS_UOM = "cams_uom"
    AOI = "aoi"
    PO_BASIN = "po_basin"
    NORTH_ITALY = "north_italy"
    NORTH_SOUTH_ITALY = "north_south_italy"
    ITALY = "italy"
    CMSAF = "cmsaf"
    CMSAF_INPUT_ID = "cmsaf_input_id"
    HSAF = "hsaf"
    HSAF_INPUT_ID = "hsaf_input_id"
    START = "start"
    END = "end"
    GROUP1 = "group1"
    GROUP2 = "group2"
    GROUP3 = "group3"


class ServiceNames(Enum):
    CLOUD_COVER = "cloud_cover"
    PRECIPITATION_AMOUNT = "precipitation_amount"
    SOLAR_RADIATION = "solar_radiation"
    AEROSOL = "aerosol"
    LST = "lst"
    SST = "sst"
    SNOW_COVER = "snow_cover"


class MapDeliveryGroups(Enum):
    CLOUD_COVER = ConfigKeys.GROUP3.value
    SOLAR_RADIATION = ConfigKeys.GROUP1.value
    PRECIPITATION_AMOUNT = ConfigKeys.GROUP3.value
    AEROSOL = ConfigKeys.GROUP2.value
    LST = ConfigKeys.GROUP1.value
    SST = ConfigKeys.GROUP1.value
    SNOW_COVER = ConfigKeys.GROUP2.value


DELIVERY_DATES = {
    ConfigKeys.GROUP1.value : {
        ConfigKeys.PO_BASIN.value : {
            ConfigKeys.START.value : "20240124",
            ConfigKeys.END.value : "20240210"
        },
        ConfigKeys.NORTH_ITALY.value : {
            ConfigKeys.START.value : "20240211",
            ConfigKeys.END.value : "20240315"
        },
        ConfigKeys.NORTH_SOUTH_ITALY.value : {
            ConfigKeys.START.value : "20240316",
            ConfigKeys.END.value : "20240416"
        },
        ConfigKeys.ITALY.value : {
            ConfigKeys.START.value : "20240417",
            ConfigKeys.END.value : "20270416"
        }
    },
    ConfigKeys.GROUP2.value : {
        ConfigKeys.PO_BASIN.value : {
            ConfigKeys.START.value : "20240124",
            ConfigKeys.END.value : "20240210"
        },
        ConfigKeys.NORTH_ITALY.value : {
            ConfigKeys.START.value : "20240211",
            ConfigKeys.END.value : "20240316"
        },
        ConfigKeys.NORTH_SOUTH_ITALY.value : {
            ConfigKeys.START.value : "20240317",
            ConfigKeys.END.value : "20240415"
        },
        ConfigKeys.ITALY.value : {
            ConfigKeys.START.value : "20240416",
            ConfigKeys.END.value : "20270416"
        }
    },
    ConfigKeys.GROUP3.value : {
        ConfigKeys.PO_BASIN.value : {
            ConfigKeys.START.value : "20240124",
            ConfigKeys.END.value : "20240209"
        },
        ConfigKeys.NORTH_ITALY.value : {
            ConfigKeys.START.value : "20240210",
            ConfigKeys.END.value : "20240315"
        },
        ConfigKeys.NORTH_SOUTH_ITALY.value : {
            ConfigKeys.START.value : "20240316",
            ConfigKeys.END.value : "20240415"
        },
        ConfigKeys.ITALY.value : {
            ConfigKeys.START.value : "20240416",
            ConfigKeys.END.value : "20270416"
        }
    }
}

def generate_directory_path(
        parent_directory : str = "/app/output",
        project_code : str = "IRIDE-S",
        product_id : str = "SX-YY-ZZ",
        reference_time : str= "yyyymmddhh",
        version : str = "V0",
) -> str:
    if parent_directory != "":
        return f"{parent_directory}/{project_code}_{product_id}_{reference_time}_{version}"
    else:
        return f"{project_code}_{product_id}_{reference_time}_{version}"


attributes_nso_gml = {
    "xmlns:ns0":"http://www.opengis.net/gml/3.2",
    "xmlns:gml":"http://www.opengis.net/gml/3.2"
}


def create_file_identifier(root, file_id):
    file_identifier = ET.SubElement(root, "gmd:fileIdentifier")
    character_string = ET.SubElement(file_identifier, "gco:CharacterString")
    character_string.text = file_id


def create_metadata_language(root, language_code):
    language = ET.SubElement(
        root, 
        "gmd:language", 
        attrib = attributes_nso_gml,
    )
    ET.SubElement(language, "gmd:LanguageCode", attrib={
        "codeList": "http://www.loc.gov/standards/iso639-2/",
        "codeListValue": language_code
    })


def create_character_set(root, character_set):
    character_set_elem = ET.SubElement(
        root,
        "gmd:characterSet",
        attrib = attributes_nso_gml,
    )
    char_set_code = ET.SubElement(character_set_elem, "gmd:MD_CharacterSetCode", attrib={
        "codeListValue": character_set,
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_CharacterSetCode"
    })
    # char_set_code.text = "UTF-8"  # Assuming it's always UTF-8


def create_resource_type(root, resource_type):
    hierarchy_level = ET.SubElement(
        root, 
        "gmd:hierarchyLevel",
        attrib = attributes_nso_gml,
    )
    ET.SubElement(hierarchy_level, "gmd:MD_ScopeCode", attrib={
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_ScopeCode",
        "codeListValue": resource_type
    })


def create_point_of_contact(root, organization_name, email):
    contact = ET.SubElement(
        root, 
        "gmd:contact",
        attrib = attributes_nso_gml,
    )
    responsible_party = ET.SubElement(contact, "gmd:CI_ResponsibleParty")

    org_name = ET.SubElement(responsible_party, "gmd:organisationName")
    org_name_text = ET.SubElement(org_name, "gco:CharacterString")
    org_name_text.text = organization_name

    contact_info = ET.SubElement(responsible_party, "gmd:contactInfo")
    ci_contact = ET.SubElement(contact_info, "gmd:CI_Contact")

    address = ET.SubElement(ci_contact, "gmd:address")
    ci_address = ET.SubElement(address, "gmd:CI_Address")

    email_address = ET.SubElement(ci_address, "gmd:electronicMailAddress")
    email_text = ET.SubElement(email_address, "gco:CharacterString")
    email_text.text = email

    role = ET.SubElement(responsible_party, "gmd:role")
    ET.SubElement(role, "gmd:CI_RoleCode", attrib={
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_RoleCode",
        "codeListValue": "pointOfContact"
    })


def get_epsg_from_tiff(tiff_file):
    with rasterio.open(tiff_file) as src:
        epsg_code = src.crs.to_epsg() if src.crs else None
    return epsg_code


def add_reference_system_info(root, tiff_file):

    if not tiff_file:
        epsg_code = "4326"
    else:
        epsg_code = get_epsg_from_tiff(tiff_file)

    if epsg_code:
        reference_system_info = ET.SubElement(
            root, 
            "gmd:referenceSystemInfo",
            attrib = attributes_nso_gml,
        )
        md_reference_system = ET.SubElement(reference_system_info, "gmd:MD_ReferenceSystem")
        reference_system_identifier = ET.SubElement(md_reference_system, "gmd:referenceSystemIdentifier")
        rs_identifier = ET.SubElement(reference_system_identifier, "gmd:RS_Identifier")
        code = ET.SubElement(rs_identifier, "gmd:code")
        character_string = ET.SubElement(code, "gco:CharacterString")
        character_string.text = f"http://www.opengis.net/def/crs/EPSG/0/{epsg_code}"
        print(epsg_code)


def get_creation_date_from_tiff(
        tiff_file: str,
    ):

    if not tiff_file:
        return datetime.datetime.now().isoformat()[:19]

    creation_date = None

    if os.path.exists(tiff_file):
        creation_date = datetime.datetime.fromtimestamp(os.path.getctime(tiff_file)).isoformat()

    return creation_date


def create_metadata_date(root, service_name : str):
    match(service_name):
        case ServiceNames.CLOUD_COVER.value | ServiceNames.SOLAR_RADIATION.value | ServiceNames.PRECIPITATION_AMOUNT.value :
            current_date = datetime.datetime.now().isoformat() #Non utilizzabile, per ora usiamo data di creazione del tif
            date_stamp = ET.SubElement(
                root, 
                "gmd:dateStamp",
                attrib = attributes_nso_gml,
            )
            date_time = ET.SubElement(date_stamp, "gco:DateTime")
            date_time.text = current_date #Non utilizzabile, per ora usiamo data di creazione del tif
        case _ :
            current_date = datetime.datetime.now().isoformat() #Non utilizzabile, per ora usiamo data di creazione del tif
            date_stamp = ET.SubElement(
                root, 
                "gmd:dateStamp",
                attrib = attributes_nso_gml,
            )
            date_time = ET.SubElement(date_stamp, "gco:DateTime")
            date_time.text = current_date
            # date_time.text = current_date[:10] #Non utilizzabile, per ora usiamo data di creazione del tif


def create_identification_info(
        root, 
        tif_file, 
        SVC, 
        PROD_ID,
        title_text,
        abstract_string,
        service_frequency,
        maintainance_delay,
        anchor_1_text,
        resolution,
        uom,
        file_data,
        begin_t, 
        end_t,
        file_name,
        shape_file_path = None,
    ):

    identification_info = ET.SubElement(
        root, 
        "gmd:identificationInfo",
        attrib = attributes_nso_gml,
    )
    data_identification = ET.SubElement(identification_info, "gmd:MD_DataIdentification")

    # Aggiungi i dettagli della citation
    citation = ET.SubElement(data_identification, "gmd:citation")
    ci_citation = ET.SubElement(citation, "gmd:CI_Citation")

    ##########################################################################################################
    title = ET.SubElement(ci_citation, "gmd:title")
    title_string = ET.SubElement(title, "gco:CharacterString")
    if tif_file:
        title_string.text = f"{PROD_ID}: {title_text}, {file_data}"
    else:
        title_string.text = f"(VOID){PROD_ID}: {title_text}, {file_data}"
    ##########################################################################################################

    # Aggiungi i dettagli della data
    date = ET.SubElement(ci_citation, "gmd:date")
    ci_date = ET.SubElement(date, "gmd:CI_Date")

    identifier = ET.SubElement(ci_citation, "gmd:identifier")
    md_identifier = ET.SubElement(identifier, "gmd:MD_Identifier")

    date_value = ET.SubElement(ci_date, "gmd:date")
    date_string = ET.SubElement(date_value, "gco:DateTime")
    date_iso = get_creation_date_from_tiff(tif_file)
    date_string.text = date_iso
    code = ET.SubElement(md_identifier, "gmd:code")
    character_string = ET.SubElement(code, "gco:CharacterString")

    # Ottieni la data di creazione in formato UNIX timestamp
    timestamp = int(datetime.datetime.fromisoformat(get_creation_date_from_tiff(tif_file)).timestamp())
    print(timestamp)
    # Aggiungi il timestamp come stringa al character_string
    character_string.text = f"{file_name.split('.')[0]}_{timestamp}"

    ##########################################################################################################
    # Informazioni sul tipo di prodotto
    abstract = ET.SubElement(data_identification, "gmd:abstract")
    abstract_text = ET.SubElement(abstract, "gco:CharacterString")
    abstract_text.text = abstract_string
    ##########################################################################################################

    # Aggiungi le informazioni aggiuntive di contatto
    additional_point_of_contact = ET.SubElement(data_identification, "gmd:pointOfContact")
    additional_responsible_party = ET.SubElement(additional_point_of_contact, "gmd:CI_ResponsibleParty")

    ##########################################################################################################
    # Da chiedere a Mario
    additional_org_name = ET.SubElement(additional_responsible_party, "gmd:organisationName")
    additional_org_name_text = ET.SubElement(additional_org_name, "gco:CharacterString")
    additional_org_name_text.text = "e-GEOS"
    ##########################################################################################################

    additional_contact_info = ET.SubElement(additional_responsible_party, "gmd:contactInfo")
    additional_ci_contact = ET.SubElement(additional_contact_info, "gmd:CI_Contact")

    additional_address = ET.SubElement(additional_ci_contact, "gmd:address")
    additional_ci_address = ET.SubElement(additional_address, "gmd:CI_Address")

    ##########################################################################################################
    # Da chiedere a Mario

    additional_email_address = ET.SubElement(additional_ci_address, "gmd:electronicMailAddress")
    additional_email_text = ET.SubElement(additional_email_address, "gco:CharacterString")
    additional_email_text.text = "vincenzo.scotti@e-geos.it"
    ##########################################################################################################

    additional_role = ET.SubElement(additional_responsible_party, "gmd:role")
    additional_role_code = ET.SubElement(additional_role, "gmd:CI_RoleCode", attrib={
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_RoleCode",
        "codeListValue": "resourceProvider"
    })
    # Adding section about resource mantainance
    resource_maintenance = ET.SubElement(data_identification, "gmd:resourceMaintenance")
    md_maintenance_information = ET.SubElement(resource_maintenance, "gmd:MD_MaintenanceInformation")

    ##########################################################################################################
    maintenance_update_frequency = ET.SubElement(md_maintenance_information, "gmd:maintenanceAndUpdateFrequency")
    md_maintenance_frequency_code = ET.SubElement(maintenance_update_frequency, "gmd:MD_MaintenanceFrequencyCode", attrib={
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_MaintenanceFrequencyCode",
        "codeListValue": service_frequency
    })
    ##########################################################################################################

    ##########################################################################################################
    # Adding next update
    date_iso = get_creation_date_from_tiff(tif_file)
    match(service_frequency):
        case "hourly":
            date_with_hour = datetime.datetime.fromisoformat(date_iso) + datetime.timedelta(hours=1)
            truncate = len("YYYY-MM-DD HH:MM:SS")
            next_date = date_with_hour.isoformat()[:truncate]
        case "daily":
            date_with_hour = datetime.datetime.fromisoformat(date_iso) + datetime.timedelta(hours=24)
            truncate = len("YYYY-MM-DD")
            next_date = date_with_hour.isoformat()[:truncate]
        case _ :
            date_with_hour = datetime.datetime.fromisoformat(date_iso) + datetime.timedelta(hours=24)
            truncate = len("YYYY-MM-DD HH:MM:SS")
            next_date = date_with_hour.isoformat()

    date_of_next_update = ET.SubElement(md_maintenance_information, "gmd:dateOfNextUpdate")
    if service_frequency == "hourly":
        date_value = ET.SubElement(date_of_next_update, "gco:DateTime")
    else:
        date_value = ET.SubElement(date_of_next_update, "gco:Date")
    date_value.text = next_date
    ##########################################################################################################

    ##########################################################################################################
    # Adding delay information
    maintenance_note = ET.SubElement(md_maintenance_information, "gmd:maintenanceNote")
    note_text = ET.SubElement(maintenance_note, "gco:CharacterString")
    note_text.text = maintainance_delay
    ##########################################################################################################

    graphic_overview = ET.SubElement(data_identification, "gmd:graphicOverview")
    md_browse_graphic = ET.SubElement(graphic_overview, "gmd:MD_BrowseGraphic")
    file__name = ET.SubElement(md_browse_graphic, "gmd:fileName")
    character_string = ET.SubElement(file__name, "gco:CharacterString")
    character_string.text = "https://dev-portal.irideservices.earth/assets/areas/S5/Hydro_Metereology.png"

    # Adding descriptive keywords (three sections)
    descriptive_keywords_1 = ET.SubElement(data_identification, "gmd:descriptiveKeywords")
    md_keywords_1 = ET.SubElement(descriptive_keywords_1, "gmd:MD_Keywords")

    keyword_1 = ET.SubElement(md_keywords_1, "gmd:keyword")
    anchor_1 = ET.SubElement(keyword_1, "gmx:Anchor", attrib={
        "xlink:href": "http://inspire.ec.europa.eu/theme/mf"
    })

    ##########################################################################################################

    anchor_1.text = anchor_1_text

    ##########################################################################################################
    thesaurus_name_1 = ET.SubElement(md_keywords_1, "gmd:thesaurusName")
    ci_citation_1 = ET.SubElement(thesaurus_name_1, "gmd:CI_Citation")

    title_1 = ET.SubElement(ci_citation_1, "gmd:title")
    title_text_1 = ET.SubElement(title_1, "gco:CharacterString")
    title_text_1.text = "GEMET - INSPIRE themes, version 1.0"

    date_1 = ET.SubElement(ci_citation_1, "gmd:date")
    ci_date_1 = ET.SubElement(date_1, "gmd:CI_Date")

    date_value_1 = ET.SubElement(ci_date_1, "gmd:date")
    date_string_1 = ET.SubElement(date_value_1, "gco:Date")
    date_string_1.text = "2008-06-01"

    date_type_1 = ET.SubElement(ci_date_1, "gmd:dateType")
    ci_date_type_1 = ET.SubElement(date_type_1, "gmd:CI_DateTypeCode", attrib={
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode",
        "codeListValue": "publication"
    })
    # ci_date_type_1.text = "Publication"

    # Seconda sezione descriptiveKeywords
    descriptive_keywords = ET.SubElement(data_identification, "gmd:descriptiveKeywords")
    md_keywords = ET.SubElement(descriptive_keywords, "gmd:MD_Keywords")

    # Prima keyword
    ta_keyword = ET.SubElement(md_keywords, "gmd:keyword")
    ta_keyword_text = ET.SubElement(ta_keyword, "gco:CharacterString")
    ta_keyword_text.text = "TA:S5"

    # Seconda keyword
    keyword = ET.SubElement(md_keywords, "gmd:keyword")
    keyword_text = ET.SubElement(keyword, "gco:CharacterString")
    keyword_text.text = F"SVC:{SVC}"

    # Terza keyword
    keyword_3 = ET.SubElement(md_keywords, "gmd:keyword")
    keyword_text_3 = ET.SubElement(keyword_3, "gco:CharacterString")
    keyword_text_3.text = f"PROD_ID:{PROD_ID}"

    # Quarta keyword
    keyword_4 = ET.SubElement(md_keywords, "gmd:keyword")
    keyword_text_4 = ET.SubElement(keyword_4, "gco:CharacterString")
    keyword_text_4.text = f"PROD_TYPE:Monitoring"

    if not tif_file:
        # Quinta keyword
        keyword_5 = ET.SubElement(md_keywords, "gmd:keyword")
        keyword_text_5 = ET.SubElement(keyword_5, "gco:CharacterString")
        keyword_text_5.text = f"PROD_CONTENT:Void"

    # Prima sezione resourceConstraints
    resource_constraints_1 = ET.SubElement(data_identification, "gmd:resourceConstraints")
    md_legal_constraints_1 = ET.SubElement(resource_constraints_1, "gmd:MD_LegalConstraints")

    use_constraints_1 = ET.SubElement(md_legal_constraints_1, "gmd:useConstraints")
    md_restriction_code_1 = ET.SubElement(use_constraints_1, "gmd:MD_RestrictionCode", attrib={
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_RestrictionCode",
        "codeListValue": "otherRestrictions"
    })

    other_constraints_1 = ET.SubElement(md_legal_constraints_1, "gmd:otherConstraints")
    character_string_1 = ET.SubElement(other_constraints_1, "gco:CharacterString")
    character_string_1.text = "Access restricted to Pilot Users for Lot 1, Industrial Team for Lot 1, ESA IPT and authorized people by ESA IPT"

    # Seconda sezione resourceConstraints
    resource_constraints_2 = ET.SubElement(data_identification, "gmd:resourceConstraints")
    md_legal_constraints_2 = ET.SubElement(resource_constraints_2, "gmd:MD_LegalConstraints")

    access_constraints_2 = ET.SubElement(md_legal_constraints_2, "gmd:accessConstraints")
    md_restriction_code_2 = ET.SubElement(access_constraints_2, "gmd:MD_RestrictionCode", attrib={
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_RestrictionCode",
    "codeListValue": "otherRestrictions"
    })
    # md_restriction_code_2.text = "otherRestrictions"

    other_constraints_2 = ET.SubElement(md_legal_constraints_2, "gmd:otherConstraints")
    anchor_2 = ET.SubElement(other_constraints_2, "gmx:Anchor", attrib={
        "xlink:href": "http://inspire.ec.europa.eu/metadata-codelist/LimitationsOnPublicAccess/INSPIRE_Directive_Article13_1d"
    })
    anchor_2.text = ("Public access to spatial data sets and services would adversely affect the confidentiality of "
                     "commercial or industrial information, where such confidentiality is provided for by national or "
                     "Community law to protect a legitimate economic interest, including the public interest in "
                     "maintaining statistical confidentiality and tax secrecy.")

    spatial_representation_type = ET.SubElement(data_identification, "gmd:spatialRepresentationType")
    md_spatial_representation_type_code = ET.SubElement(spatial_representation_type, "gmd:MD_SpatialRepresentationTypeCode", attrib={
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_SpatialRepresentationTypeCode",
        "codeListValue": "grid"
    })
    # md_spatial_representation_type_code.text = "Grid"

    ##########################################################################################################
    # Sezione spatialResolution
    spatial_resolution = ET.SubElement(data_identification, "gmd:spatialResolution")
    md_resolution = ET.SubElement(spatial_resolution, "gmd:MD_Resolution")
    distance = ET.SubElement(md_resolution, "gmd:distance")
    distance_value = ET.SubElement(distance, "gco:Distance", attrib={"uom": uom})
    distance_value.text = resolution
    ##########################################################################################################

    # Sezione language
    language = ET.SubElement(data_identification, "gmd:language")
    language_code = ET.SubElement(language, "gmd:LanguageCode", attrib={
        "codeList": "http://www.loc.gov/standards/iso639-2/",
        "codeListValue": "eng"
    })
    language_code.text = "English"

    language = ET.SubElement(data_identification, "gmd:topicCategory")
    language_code = ET.SubElement(language, "gmd:MD_TopicCategoryCode")
    language_code.text = "imageryBaseMapsEarthCover"

    if tif_file:
        with rasterio.open(tif_file) as src:
            bounds = src.bounds  # Ottenimento delle coordinate della bounding box

        # Estrazione delle coordinate dalla bounding box
        west_bound_longitude = bounds.left
        east_bound_longitude = bounds.right
        south_bound_latitude = bounds.bottom
        north_bound_latitude = bounds.top

    elif shape_file_path:
        # Using geopandas to read the shapefile
        gdf = gpd.read_file(shape_file_path)
        # Getting the bounding box from the GeoDataFrame
        bounds = gdf.total_bounds

        # Estrazione delle coordinate dalla bounding box
        west_bound_longitude = bounds[0]
        south_bound_latitude = bounds[1]
        east_bound_longitude = bounds[2]
        north_bound_latitude = bounds[3]

    # Sezione extent (Bounding Box)
    extent = ET.SubElement(data_identification, "gmd:extent")
    ex_extent = ET.SubElement(extent, "gmd:EX_Extent")

    # Elemento geographicElement
    geographic_element = ET.SubElement(ex_extent, "gmd:geographicElement")
    ex_geographic_bounding_box = ET.SubElement(geographic_element, "gmd:EX_GeographicBoundingBox")

    # Impostazione delle coordinate estratte
    west_bound_longitude_elem = ET.SubElement(ex_geographic_bounding_box, "gmd:westBoundLongitude")
    west_bound_longitude_decimal = ET.SubElement(west_bound_longitude_elem, "gco:Decimal")
    west_bound_longitude_decimal.text = str(west_bound_longitude)

    east_bound_longitude_elem = ET.SubElement(ex_geographic_bounding_box, "gmd:eastBoundLongitude")
    east_bound_longitude_decimal = ET.SubElement(east_bound_longitude_elem, "gco:Decimal")
    east_bound_longitude_decimal.text = str(east_bound_longitude)

    south_bound_latitude_elem = ET.SubElement(ex_geographic_bounding_box, "gmd:southBoundLatitude")
    south_bound_latitude_decimal = ET.SubElement(south_bound_latitude_elem, "gco:Decimal")
    south_bound_latitude_decimal.text = str(south_bound_latitude)

    north_bound_latitude_elem = ET.SubElement(ex_geographic_bounding_box, "gmd:northBoundLatitude")
    north_bound_latitude_decimal = ET.SubElement(north_bound_latitude_elem, "gco:Decimal")
    north_bound_latitude_decimal.text = str(north_bound_latitude)

    ##########################################################################################################
    # Chiesto a Mario
    # Section about time coverage of the data
    temporal_element = ET.SubElement(ex_extent, "gmd:temporalElement")
    ex_temporal_extent = ET.SubElement(temporal_element, "gmd:EX_TemporalExtent")
    extent_element = ET.SubElement(ex_temporal_extent, "gmd:extent")
    time_period = ET.SubElement(extent_element, "gml:TimePeriod",
                                attrib={"gml:id": "IDcd3b1c4f-b5f7-439a-afc4-3317a4cd89be"})
    ##########################################################################################################
    # Aggiorna le informazioni di inizio e fine nel documento XML
    begin_position = ET.SubElement(time_period, "gml:beginPosition")
    begin_position.text = begin_t

    end_position = ET.SubElement(time_period, "gml:endPosition")
    end_position.text = end_t

    date_type = ET.SubElement(ci_date, "gmd:dateType")
    ci_date_type = ET.SubElement(date_type, "gmd:CI_DateTypeCode", attrib={
        "codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode",
        "codeListValue": "creation"
    })
    # ci_date_type.text = "Creation"


def add_distribution_info(
        root, 
        tif_file : bool = True,
        product_url : str = "https://webgis-s1-dev.irideservices.earth/geoserver/ows",
):
    distribution_info = ET.SubElement(
        root, 
        "gmd:distributionInfo",
        attrib = attributes_nso_gml,
    )

    md_distribution = ET.SubElement(distribution_info, "gmd:MD_Distribution")

    # Aggiungi le informazioni sul formato di distribuzione
    distribution_format = ET.SubElement(md_distribution, "gmd:distributionFormat")
    md_format = ET.SubElement(distribution_format, "gmd:MD_Format")
    name = ET.SubElement(md_format, "gmd:name")
    name_string = ET.SubElement(name, "gco:CharacterString")
    name_string.text = "tif"

    version = ET.SubElement(md_format, "gmd:version", attrib={"gco:nilReason": "unknown"})

    # Aggiungi le opzioni di trasferimento online
    transfer_options = ET.SubElement(md_distribution, "gmd:transferOptions")
    digital_transfer_options = ET.SubElement(transfer_options, "gmd:MD_DigitalTransferOptions")

    # Aggiungi le risorse online
    online_resources = [
        {
            "url": product_url,
            "protocol": "WWW:LINK-1.0-http--link"
        },
        {
            "url": "https://landservices-dev.iride.earth/download_products/",
            "protocol": "WWW:DOWNLOAD-1.0-S3--download"
        },
        {
            "url": "https://landservices-dev.iride.earth/account/login/",
            "protocol": "OGC:WMS",
            "name": "WMS",
            "description": "Once you click on the WMS link, enter the credentials on the webpage, and then copy the "
                           "link obtained by clicking on 'use layers WMS' on the webpage into QGIS. "
                           "This will allow you to view the layers in WMS mode."
        }
    ]

    for resource in online_resources:
        online = ET.SubElement(digital_transfer_options, "gmd:onLine")
        ci_online_resource = ET.SubElement(online, "gmd:CI_OnlineResource")

        linkage = ET.SubElement(ci_online_resource, "gmd:linkage")
        url = ET.SubElement(linkage, "gmd:URL")
        url.text = resource["url"] if tif_file else "Not Applicable"

        protocol = ET.SubElement(ci_online_resource, "gmd:protocol")
        protocol_string = ET.SubElement(protocol, "gco:CharacterString")
        protocol_string.text = resource["protocol"]

        if "name" in resource and resource["name"]:
            name = ET.SubElement(ci_online_resource, "gmd:name")
            name_string = ET.SubElement(name, "gco:CharacterString")
            name_string.text = resource["name"] if tif_file else "Not Applicable"

        if "description" in resource and resource["description"]:
            description = ET.SubElement(ci_online_resource, "gmd:description")
            description_string = ET.SubElement(description, "gco:CharacterString")
            description_string.text = resource["description"]


def add_data_quality_info(
        root,
        positional_accuracy,
        thematic_accuracy,
        statement_text,
        tif_file : bool = True,
    ):
    data_quality_info = ET.SubElement(
        root, 
        "gmd:dataQualityInfo",
        attrib = attributes_nso_gml,
    )
    dq_data_quality = ET.SubElement(data_quality_info, "gmd:DQ_DataQuality")

    # Aggiungi informazioni sulla portata
    scope = ET.SubElement(dq_data_quality, "gmd:scope")
    dq_scope = ET.SubElement(scope, "gmd:DQ_Scope")
    level = ET.SubElement(dq_scope, "gmd:level")
    md_scope_code = ET.SubElement(level, "gmd:MD_ScopeCode",
                                  attrib={"codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_ScopeCode",
                                          "codeListValue": "dataset"})

    # Aggiungi rapporti sulla qualità dei dati
    reports = [
        {
            "title": "Commission Regulation (EU) No 1089/2010 of 23 November 2010 implementing Directive 2007/2/EC of the European Parliament and of the Council as regards interoperability of spatial data sets and services",
            "date": "2010-12-08",
            "date_type": "publication",
            "explanation": "See the referenced specification",
            "pass": "unknown"
        },
        {
            "title": "Service Portfolio Design and Service Value Chains Design",
            "date": "2023-11-15",
            "date_type": "publication",
            "explanation": "The product conforms to technical specifications declared in the service portfolio design and service value chains design deliverables",
            "pass": "true"
        }
    ]

    gmd_report = ET.SubElement(dq_data_quality, "gmd:report")
    gmd_dq_domain_consistency = ET.SubElement(gmd_report, "gmd:DQ_DomainConsistency")

    for report in reports:
        gmd_result = ET.SubElement(gmd_dq_domain_consistency, "gmd:result")
        dq_conformance_result = ET.SubElement(gmd_result, "gmd:DQ_ConformanceResult")
        specification = ET.SubElement(dq_conformance_result, "gmd:specification")
        ci_citation = ET.SubElement(specification, "gmd:CI_Citation")
        title = ET.SubElement(ci_citation, "gmd:title")
        title_string = ET.SubElement(title, "gco:CharacterString")
        title_string.text = report["title"]

        date = ET.SubElement(ci_citation, "gmd:date")
        ci_date = ET.SubElement(date, "gmd:CI_Date")
        gmd_date = ET.SubElement(ci_date, "gmd:date")
        date_value = ET.SubElement(gmd_date, "gco:Date")
        date_value.text = report["date"]
        date_type = ET.SubElement(ci_date, "gmd:dateType")
        date_type_code = ET.SubElement(date_type, "gmd:CI_DateTypeCode",
                                       attrib={"codeList": "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_DateTypeCode",
                                               "codeListValue": report["date_type"]})

        explanation = ET.SubElement(dq_conformance_result, "gmd:explanation")
        explanation_string = ET.SubElement(explanation, "gco:CharacterString")
        explanation_string.text = report["explanation"]

        ET.SubElement(dq_conformance_result, "gmd:pass", attrib={"gco:nilReason":"unknown"})
        # boolean_element = ET.SubElement(pass_element, "gco:Boolean")
        # boolean_element.text = "true"

    # Aggiungi informazioni sulla classificazione tematica
    if tif_file:
        thematic_classification_correctness = ET.SubElement(dq_data_quality, "gmd:report")
        thematic_classification = ET.SubElement(thematic_classification_correctness, "gmd:DQ_ThematicClassificationCorrectness")
        name_of_measure = ET.SubElement(thematic_classification, "gmd:nameOfMeasure")
        name_of_measure_string = ET.SubElement(name_of_measure, "gco:CharacterString")
        name_of_measure_string.text = "Rate of correct attribute values"

        quantitative_result = ET.SubElement(thematic_classification, "gmd:result")
        dq_quantitative_result = ET.SubElement(quantitative_result, "gmd:DQ_QuantitativeResult")
        value_type = ET.SubElement(dq_quantitative_result, "gmd:valueType")
        record_type = ET.SubElement(value_type, "gco:RecordType")
        record_type.text = "Percentage"
        value_unit = ET.SubElement(dq_quantitative_result, "gmd:valueUnit")
        unit_definition = ET.SubElement(value_unit, "gml:UnitDefinition", attrib={"xmlns:gml":"http://www.opengis.net/gml/3.2", "gml:id":"d170783e338a2100510"})
        identifier = ET.SubElement(unit_definition, "gml:identifier", codeSpace="")
        identifier.text = "percentage"
        value = ET.SubElement(dq_quantitative_result, "gmd:value")
        record = ET.SubElement(value, "gco:Record")
        record.text = thematic_accuracy

        # Aggiungi informazioni sull'accuratezza attributiva quantitativa
        quantitative_attribute_accuracy = ET.SubElement(dq_data_quality, "gmd:report")
        attribute_accuracy = ET.SubElement(quantitative_attribute_accuracy, "gmd:DQ_QuantitativeAttributeAccuracy")
        name_of_measure = ET.SubElement(attribute_accuracy, "gmd:nameOfMeasure")
        name_of_measure_string = ET.SubElement(name_of_measure, "gco:CharacterString")
        name_of_measure_string.text = "Positional Accuracy"

        ##########################################################################################################
        quantitative_result = ET.SubElement(attribute_accuracy, "gmd:result")
        dq_quantitative_result = ET.SubElement(quantitative_result, "gmd:DQ_QuantitativeResult")
        value_type = ET.SubElement(dq_quantitative_result, "gmd:valueType")
        record_type = ET.SubElement(value_type, "gco:RecordType")
        record_type.text = "Measure(s) (value(s) + unit(s))"
        value_unit = ET.SubElement(dq_quantitative_result, "gmd:valueUnit")
        unit_definition = ET.SubElement(value_unit, "gml:UnitDefinition", attrib={"xmlns:gml":"http://www.opengis.net/gml/3.2", "gml:id":"d170783e338a2100510"})
        identifier = ET.SubElement(unit_definition, "gml:identifier", codeSpace="")
        identifier.text = "measure"
        value = ET.SubElement(dq_quantitative_result, "gmd:value")
        record = ET.SubElement(value, "gco:Record")
        record.text = f"> {positional_accuracy}%"
    ###########################################CHANGE#################################################

    # Aggiungi informazioni sulla lineage
    lineage = ET.SubElement(dq_data_quality, "gmd:lineage")
    li_lineage = ET.SubElement(lineage, "gmd:LI_Lineage")
    statement = ET.SubElement(li_lineage, "gmd:statement")
    statement_string = ET.SubElement(statement, "gco:CharacterString")
    statement_string.text = statement_text


def metadata_generation(
        xml_configs : dict,
        input_text_path : str,
):
    """
    Generate metadata for .tif file placed into specified directory path : xml_configs["xml_folder"].
    xml_configs should be structured as the following example (Populate with specific service's info).

    xml_configs = {
        "svc" : "S5-02",
        "product_id" : "S5-02-06",
        "xml_folder" : "C:\\Users\\gimbo\\OneDrive\\Desktop\\IrideProducts\\cloud_cover\\output\\IRIDE-S_S5-02-06_2024012401_V0\\cropped_tiff",
        "service_name" : "cloud_cover",
        "anchor_1_text" : "Cloud cover",
        "title_text" : "Cloud Cover binary mask over Po Basin",.",
        "service_frequency" : "hourly",
        "maintainance_delay" : "Time needed for availability of input data: min <=3h, max 1 day, depending on the availability of the input used for the generation",
        "resolution" : "0.5",
        "uom" : "deg",
        "positional_accuracy" : "80",
        "thematic_accuracy" : "F1 Score >= 80%",
        "statement_text" : "Mapping of Cloud Cover based on EO data over the Po Basin Region"
    }
    """

    svc = xml_configs.get(ConfigKeys.SVC.value)
    prod_id = xml_configs.get(ConfigKeys.PRODUCT_ID.value)
    xml_folder = xml_configs.get(ConfigKeys.XML_FOLDER.value)
    service_name = xml_configs.get(ConfigKeys.SERVICE_NAME.value)
    aoi = xml_configs.get(ConfigKeys.AOI.value)
    title_text = xml_configs[ConfigKeys.TITLE_TEXT.value][aoi]
    abstract_string = xml_configs[ConfigKeys.ABSTRACT_STRING.value][aoi]
    service_frequency = xml_configs.get(ConfigKeys.SERVICE_FREQUENCY.value)
    maintainance_delay = xml_configs.get(ConfigKeys.MAINTAINANCE_DELAY.value)
    anchor_1_text = xml_configs.get(ConfigKeys.ANCHOR_1_TEXT.value)
    resolution = xml_configs.get(ConfigKeys.RESOLUTION.value)
    uom = xml_configs.get(ConfigKeys.UOM.value)
    thematic_accuracy = xml_configs.get(ConfigKeys.THEMATIC_ACCURACY.value)
    positional_accuracy = xml_configs.get(ConfigKeys.POSITIONAL_ACCURACY.value)
    statement_text = xml_configs[ConfigKeys.STATEMENT_TEXT.value][aoi]
    product_url = xml_configs.get(ConfigKeys.PRODUCT_URL.value)
    input_files_list = xml_configs.get(ConfigKeys.INPUT_FILES_LIST.value)
    shapefile_path = xml_configs[ConfigKeys.SHAPEFILE_PATH.value][aoi]
    reference_time = xml_configs.get(ConfigKeys.REFERENCE_TIME.value) #"YYYYMMDDHH"

    with open(input_text_path, "r", encoding="utf-8") as src:
        input_text = src.read()

    statement_text = f"{statement_text}\n{input_text}"

    # Creazione dell'elemento radice

    root = ET.Element(
        "gmd:MD_Metadata",
        attrib={
            "xmlns:gmd": "http://www.isotc211.org/2005/gmd",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xmlns:gco": "http://www.isotc211.org/2005/gco",
            "xmlns:srv": "http://www.isotc211.org/2005/srv",
            "xmlns:gmx": "http://www.isotc211.org/2005/gmx",
            "xmlns:gts": "http://www.isotc211.org/2005/gts", 
            "xmlns:gsr": "http://www.isotc211.org/2005/gsr",
            "xmlns:gmi": "http://www.isotc211.org/2005/gmi",
            "xmlns:gml": "http://www.opengis.net/gml",
            "xmlns:xlink": "http://www.w3.org/1999/xlink",
            "xsi:schemaLocation": "http://www.isotc211.org/2005/gmd http://schemas.opengis.net/iso/19139/20060504/gmd/gmd.xsd"
        }
    )

    # Aggiungi i metadati richiesti
    file_name = generate_directory_path(
        parent_directory = "",
        product_id = prod_id,
        reference_time = reference_time,
    )

    print(file_name)

    tiff_files = glob.glob(os.path.join(xml_folder, f'{file_name}.tif'))
    print(tiff_files)
    tiff_file_path = tiff_files[0]

    # Aggiungi i metadati richiesti
    file_name = os.path.basename(tiff_file_path)
    end_time_str = file_name.split('_')[2]  # "YYYMMDDHH"

    # Format the datetime objects back to strings YYYY-MM-DDTHH:MM:SS
    match (service_name):
        case ServiceNames.CLOUD_COVER.value | ServiceNames.SOLAR_RADIATION.value | ServiceNames.PRECIPITATION_AMOUNT.value : 
                # Convert end_time_str to datetime object
            end_time : datetime.datetime = datetime.datetime.strptime(end_time_str, '%Y%m%d%H')
            # Calculate begin_time by subtracting one hour
            begin_time : datetime.datetime = (end_time - datetime.timedelta(hours = 1))

            formatted_end_time = end_time.strftime('%Y-%m-%dT%H:%M:%S')
            formatted_begin_time = begin_time.strftime('%Y-%m-%dT%H:%M:%S')
            formatted_begin_time2 = begin_time.strftime("%d/%m/%YT%H:%M")
        case _:
                # Convert end_time_str to datetime object
            end_time : datetime.datetime = datetime.datetime.strptime(end_time_str, '%Y%m%d')
            # Calculate begin_time by subtracting one hour
            begin_time : datetime.datetime = (end_time - datetime.timedelta(hours = 24))

            formatted_begin_time = f"{end_time.strftime('%Y-%m-%d')}T00:00:00"
            formatted_end_time = f"{end_time.strftime('%Y-%m-%d')}T23:59:59"
            formatted_begin_time2 = end_time.strftime("%d/%m/%Y")

    get_creation_date_from_tiff(
        tiff_file = tiff_file_path,
    )

    create_file_identifier(root, file_name.split('.')[0]) #TODO Ask if ok
    create_metadata_language(root, "eng")
    create_character_set(root, "utf8")
    create_resource_type(root, "dataset")
    create_point_of_contact(root, "GEO-K", "s5_02@geo-k.co")
    create_metadata_date(root, service_name = service_name)
    add_reference_system_info(root, tiff_file_path)
    create_identification_info(
        root,
        tiff_file_path,
        SVC = svc,
        PROD_ID = prod_id,
        title_text = title_text,
        abstract_string = abstract_string,
        service_frequency = service_frequency,
        maintainance_delay = maintainance_delay,
        anchor_1_text = anchor_1_text,
        resolution = resolution,
        uom = uom,
        file_data = formatted_begin_time2,
        begin_t = formatted_begin_time,
        end_t = formatted_end_time,
        file_name = file_name,
    )
    add_distribution_info(root, product_url= product_url)
    add_data_quality_info(
        root, 
        positional_accuracy = positional_accuracy, 
        thematic_accuracy = thematic_accuracy, 
        statement_text = statement_text,
    )
    
    # Creazione dell'albero XML
    metadata_tree = ET.ElementTree(root)
    
    # Formattazione dell'albero con indentazioni corrette
    metadata_string = ET.tostring(root, encoding="utf-8", xml_declaration = False)
    formatted_metadata = minidom.parseString(metadata_string).toprettyxml(indent="  ")

    # Remove the XML declaration from formatted_metadata
    formatted_metadata_lines = formatted_metadata.split('\n')[1:]

    # Join the lines back together
    formatted_metadata_without_declaration = '\n'.join(formatted_metadata_lines)

    #file_path = xml_folder + f"IRIDE-S_{PROD_ID}_{begin_time_str}_{end_time_str}.xml"
    file_path = tiff_file_path.replace('.tif', '.xml')
    with open(file_path, "w", encoding="utf-8") as xml_file:
        xml_file.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        xml_file.write(formatted_metadata_without_declaration)

    root = ET.Element(
        "gmd:MD_Metadata",
        attrib={
            "xmlns:gmd": "http://www.isotc211.org/2005/gmd",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xmlns:gco": "http://www.isotc211.org/2005/gco",
            "xmlns:srv": "http://www.isotc211.org/2005/srv",
            "xmlns:gmx": "http://www.isotc211.org/2005/gmx",
            "xmlns:gts": "http://www.isotc211.org/2005/gts", 
            "xmlns:gsr": "http://www.isotc211.org/2005/gsr",
            "xmlns:gmi": "http://www.isotc211.org/2005/gmi",
            "xmlns:gml": "http://www.opengis.net/gml",
            "xmlns:xlink": "http://www.w3.org/1999/xlink",
            "xsi:schemaLocation": "http://www.isotc211.org/2005/gmd http://schemas.opengis.net/iso/19139/20060504/gmd/gmd.xsd"
        }
    )


def void_metadata_generation(
        xml_configs : dict,
):
    """
    Generate metadata for .tif file placed into specified directory path : xml_configs["xml_folder"].
    xml_configs should be structured as the following example (Populate with specific service's info).

    xml_configs = {
        "svc" : "S5-02",
        "product_id" : "S5-02-06",
        "xml_folder" : "C:\\Users\\gimbo\\OneDrive\\Desktop\\IrideProducts\\cloud_cover\\output\\IRIDE-S_S5-02-06_2024012401_V0\\cropped_tiff",
        "service_name" : "cloud_cover",
        "anchor_1_text" : "Cloud cover",
        "title_text" : "Cloud Cover binary mask over Po Basin",
        "abstract_string" : "Cloud cover mapping over the Po' basin, using a binary mask, with hourly temporal resolution. The thematic accuracy (F1 score) of the product is greater than 80% and the positional accuracy is greater than 80% of pixel size at 2&#963; confidence level.",
        "service_frequency" : "hourly",
        "maintainance_delay" : "Time needed for availability of input data: min <=3h, max 1 day, depending on the availability of the input used for the generation",
        "resolution" : "0.5",
        "uom" : "deg",
        "positional_accuracy" : "80",
        "thematic_accuracy" : "F1 Score >= 80%",
        "statement_text" : "Mapping of Cloud Cover based on EO data over the Po Basin Region"
    }
    """

    svc = xml_configs.get(ConfigKeys.SVC.value)
    prod_id = xml_configs.get(ConfigKeys.PRODUCT_ID.value)
    xml_folder = xml_configs.get(ConfigKeys.XML_FOLDER.value)
    service_name = xml_configs.get(ConfigKeys.SERVICE_NAME.value)
    aoi = xml_configs.get(ConfigKeys.AOI.value)
    title_text = xml_configs[ConfigKeys.TITLE_TEXT.value][aoi]
    abstract_string = "The expected product has not been delivered following concerns regarding input data."
    service_frequency = xml_configs.get(ConfigKeys.SERVICE_FREQUENCY.value)
    maintainance_delay = xml_configs.get(ConfigKeys.MAINTAINANCE_DELAY.value)
    anchor_1_text = xml_configs.get(ConfigKeys.ANCHOR_1_TEXT.value)
    resolution = xml_configs.get(ConfigKeys.RESOLUTION.value)
    uom = xml_configs.get(ConfigKeys.UOM.value)
    thematic_accuracy = xml_configs.get(ConfigKeys.THEMATIC_ACCURACY.value)
    positional_accuracy = xml_configs.get(ConfigKeys.POSITIONAL_ACCURACY.value)
    statement_text = xml_configs[ConfigKeys.STATEMENT_TEXT.value][aoi]
    product_url = xml_configs.get(ConfigKeys.PRODUCT_URL.value)
    input_files_list = xml_configs.get(ConfigKeys.INPUT_FILES_LIST.value)
    shapefile_path = xml_configs[ConfigKeys.SHAPEFILE_PATH.value][aoi]
    reference_time = xml_configs.get(ConfigKeys.REFERENCE_TIME.value) #"YYYYMMDDHH"

    statement_text = f"{statement_text}\nInput data:\nNot Available"

    # Creazione dell'elemento radice

    root = ET.Element(
        "gmd:MD_Metadata",
        attrib={
            "xmlns:gmd": "http://www.isotc211.org/2005/gmd",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xmlns:gco": "http://www.isotc211.org/2005/gco",
            "xmlns:srv": "http://www.isotc211.org/2005/srv",
            "xmlns:gmx": "http://www.isotc211.org/2005/gmx",
            "xmlns:gts": "http://www.isotc211.org/2005/gts", 
            "xmlns:gsr": "http://www.isotc211.org/2005/gsr",
            "xmlns:gmi": "http://www.isotc211.org/2005/gmi",
            "xmlns:gml": "http://www.opengis.net/gml",
            "xmlns:xlink": "http://www.w3.org/1999/xlink",
            "xsi:schemaLocation": "http://www.isotc211.org/2005/gmd http://schemas.opengis.net/iso/19139/20060504/gmd/gmd.xsd"
        }
    )

    # Aggiungi i metadati richiesti
    file_name = generate_directory_path(
        parent_directory = "",
        product_id = prod_id,
        reference_time = reference_time,
    )

    end_time_str = file_name.split('_')[2]  # "YYYMMDDHH"

    # Format the datetime objects back to strings YYYY-MM-DDTHH:MM:SS
    match (service_name):
        case ServiceNames.CLOUD_COVER.value | ServiceNames.SOLAR_RADIATION.value | ServiceNames.PRECIPITATION_AMOUNT.value : 
                # Convert end_time_str to datetime object
            end_time : datetime.datetime = datetime.datetime.strptime(end_time_str, '%Y%m%d%H')
            # Calculate begin_time by subtracting one hour
            begin_time : datetime.datetime = (end_time - datetime.timedelta(hours = 1))

            formatted_end_time = end_time.strftime('%Y-%m-%dT%H:%M:%S')
            formatted_begin_time = begin_time.strftime('%Y-%m-%dT%H:%M:%S')
            formatted_begin_time2 = begin_time.strftime("%d/%m/%YT%H:%M")
        case _:
                # Convert end_time_str to datetime object
            end_time : datetime.datetime = datetime.datetime.strptime(end_time_str, '%Y%m%d')
            # Calculate begin_time by subtracting one hour
            begin_time : datetime.datetime = (end_time - datetime.timedelta(hours = 24))

            formatted_begin_time = f"{end_time.strftime('%Y-%m-%d')}T00:00:00"
            formatted_end_time = f"{end_time.strftime('%Y-%m-%d')}T23:59:59"
            formatted_begin_time2 = end_time.strftime("%d/%m/%Y")

    tiff_file_path = None # set to None since there's no tif product for this xml.

    create_file_identifier(root, file_name.split('.')[0]) #TODO Ask if ok
    create_metadata_language(root, "eng")
    create_character_set(root, "utf8")
    create_resource_type(root, "dataset")
    create_point_of_contact(root, "GEO-K", "s5_02@geo-k.co")
    create_metadata_date(root, service_name = service_name)
    add_reference_system_info(root, tiff_file_path)
    create_identification_info(
        root,
        tiff_file_path,
        SVC = svc,
        PROD_ID = prod_id,
        title_text = title_text,
        abstract_string = abstract_string,
        service_frequency = service_frequency,
        maintainance_delay = maintainance_delay,
        anchor_1_text = anchor_1_text,
        resolution = resolution,
        uom = uom,
        file_data = formatted_begin_time2,
        begin_t = formatted_begin_time,
        end_t = formatted_end_time,
        file_name = file_name,
        shape_file_path = shapefile_path,
    )
    add_distribution_info(root, tif_file = False)
    add_data_quality_info(
        root, 
        positional_accuracy = positional_accuracy, 
        thematic_accuracy = thematic_accuracy, 
        statement_text = statement_text,
        tif_file = False,
    )

    # Creazione dell'albero XML
    metadata_tree = ET.ElementTree(root)
    
    # Formattazione dell'albero con indentazioni corrette
    metadata_string = ET.tostring(root, encoding="utf-8", xml_declaration = False)
    formatted_metadata = minidom.parseString(metadata_string).toprettyxml(indent="  ")

    # Remove the XML declaration from formatted_metadata
    formatted_metadata_lines = formatted_metadata.split('\n')[1:]

    # Join the lines back together
    formatted_metadata_without_declaration = '\n'.join(formatted_metadata_lines)

    #file_path = xml_folder + f"IRIDE-S_{PROD_ID}_{begin_time_str}_{end_time_str}.xml"  # Specifica il percorso del file e il suo nome
    if not os.path.exists(xml_folder):
        os.makedirs(xml_folder)
    file_path = os.path.join(xml_folder, f"{file_name}.xml")
    with open(file_path, "w", encoding="utf-8") as xml_file:
        xml_file.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        xml_file.write(formatted_metadata_without_declaration)

    root = ET.Element(
        "gmd:MD_Metadata",
        attrib={
            "xmlns:gmd": "http://www.isotc211.org/2005/gmd",
            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "xmlns:gco": "http://www.isotc211.org/2005/gco",
            "xmlns:srv": "http://www.isotc211.org/2005/srv",
            "xmlns:gmx": "http://www.isotc211.org/2005/gmx",
            "xmlns:gts": "http://www.isotc211.org/2005/gts", 
            "xmlns:gsr": "http://www.isotc211.org/2005/gsr",
            "xmlns:gmi": "http://www.isotc211.org/2005/gmi",
            "xmlns:gml": "http://www.opengis.net/gml",
            "xmlns:xlink": "http://www.w3.org/1999/xlink",
            "xsi:schemaLocation": "http://www.isotc211.org/2005/gmd http://schemas.opengis.net/iso/19139/20060504/gmd/gmd.xsd"
        }
    )


def get_aoi_by_date(year: int, day: int, month: int, delivery_group: str = ConfigKeys.GROUP1.value):
    target_date = datetime.datetime(year, month, day)
    print("Target Date:", target_date)

    for aoi, dates in DELIVERY_DATES[delivery_group].items():
        start_date = datetime.datetime.strptime(dates[ConfigKeys.START.value], "%Y%m%d")
        end_date = datetime.datetime.strptime(dates[ConfigKeys.END.value], "%Y%m%d")
        print("Start Date:", start_date, "End Date:", end_date)
        if start_date <= target_date <= end_date:
            return aoi

    return None


def extract_year_month_day(date_string : str):
    date_obj = datetime.datetime.strptime(date_string, "%Y%m%d")
    year = date_obj.year
    month = date_obj.month
    day = date_obj.day
    return year, month, day


def get_delivery_group_by_service_name(service: str):
    match(service):
        case ServiceNames.CLOUD_COVER.value:
            return MapDeliveryGroups.CLOUD_COVER.value
        case ServiceNames.SOLAR_RADIATION.value:
            return MapDeliveryGroups.SOLAR_RADIATION.value
        case ServiceNames.PRECIPITATION_AMOUNT.value:
            return MapDeliveryGroups.PRECIPITATION_AMOUNT.value
        case ServiceNames.AEROSOL.value:
            return MapDeliveryGroups.AEROSOL.value
        case ServiceNames.LST.value:
            return MapDeliveryGroups.LST.value
        case ServiceNames.SST.value:
            return MapDeliveryGroups.SST.value
        case ServiceNames.SNOW_COVER.value:
            return MapDeliveryGroups.SNOW_COVER.value

    return None



if __name__ == "__main__" :

    import warnings
    warnings.filterwarnings("ignore")

    
    root_path =""

    XML_CONFIGS = {
            "svc" : "S5-02",
            "product_id" : "S5-02-05",
            "xml_folder" : "/media/fabspace/Volume/IRIDE/IrideMetadata/S5-02-05",
            "xml_parent" : "/media/fabspace/Volume/IRIDE/IrideMetadata",
            "service_name" : "snow_cover",
            "anchor_1_text" : "Meteorological geographical features",
            "title_text" : {
                "po_basin" : "Snow Cover binary mask, Po' Basin",
                "north_italy" : "Snow Cover binary mask, North Italy",
                "north_south_italy" : "Snow Cover binary mask, North and South of Italy",
                "italy" : "Snow Cover binary mask, Italy"
            },
            "abstract_string" : {
                "po_basin" : "Delivery: 1 of 4 - Partial AOI: Po' basin - Precursor AOI: Italy",
                "north_italy" : "Delivery: 2 of 4 - Partial AOI: North of Italy - Precursor AOI: Italy",
                "north_south_italy" : "Delivery: 3 of 4 - Partial AOI: North and South of Italy - Precursor AOI: Italy",
                "italy" : "Delivery: 4 of 4 - Italy"
            },
            "service_frequency" : "daily",
            "maintainance_delay" : "Time needed for availability of input data: min <= 1 day, max 6 days, depending on the availability of Sentinel-3, HSAF and ERA5",
            "resolution" : "3000-10000",
            "uom" : "m",
            "positional_accuracy" : "80",
            "thematic_accuracy" : "F1 score >= 80%",
            "statement_text" : {
                "po_basin" : "Mapping of Snow Cover based on EO data over the Po Basin Region",
                "north_italy" : "Mapping of Snow Cover based on EO data over the North of Italy",
                "north_south_italy" : "Mapping of Snow Cover based on EO data over the North and South of Italy",
                "italy" : "Mapping of Snow Cover based on EO data over Italy"
            },
            "product_url" : "https://landservices-dev.iride.earth/catalogue/#/dataset/2",
            "input_files_list" : [],
            "shapefile_path" : {
                "po_basin" : "/app/common/shapefiles/Bacino_fiume_Po/Bacino_fiume_Po.shp",
                "north_italy" : "/app/common/shapefiles/Nord_Italia_AOD/prova1.shp",
                "north_south_italy" : "/app/common/shapefiles/Nord_Sud_Italia/nord_sud_sicilia.shp",
                "italy" : "/app/common/shapefiles/Nord_Sud_Italia/nord_sud_sicilia.shp"
            },
            "reference_time" : "",
            "aoi" : "po_basin",
            "hsaf_input_id" : "[IN-S5-02-15] HSAF",
            "era5_input_id" : "[IN-S5-02-20] ERA5/SNOWCOVER",
            "s3_input_id_l1" : "[IN-S5-02-02] Sentinel-3/L1 OLCI",
            "s3_input_id_l2" : "[IN-S5-02-02] Sentinel-3/L2 OLCI",
            "s3_l1_resolution" : "300",
            "s3_l1_uom" : "m",
            "s3_l2_resolution" : "300",
            "s3_l2_uom" : "m",
            "hsaf_resolution" : "1113",
            "hsaf_uom" : "m",
            "era5_resolution" : "9000",
            "era5_uom" : "m"
        }
    
    date_strs = [ "20240224", "20240225", "20240226"]
    for date_str in date_strs:
        
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:])
        #REFERENCE_TIME = "20240222"
        REFERENCE_TIME = f"{year}{month:02d}{day:02d}"
    
        # Example usage:
        my_prod_version_dir="IRIDE_SNOWCOVER_DATAFUSION_PRODUCTS"#"IRIDE_SNOWCOVER_DATAFUSION_PRODUCTS"
        source_dir = os.path.join(root_path, f"{my_prod_version_dir}/{year}-{month:02d}-{day:02d}/")
        destination_dir = os.path.join(root_path, f"{my_prod_version_dir}/products_tiffs/")
        #copy_tif_files(source_dir, destination_dir)

        product_tiff_path=os.path.join(destination_dir,f"IRIDE-S_S5-02-05_{date_str}_V0.tif")
        #process_nan_values(product_tiff_path)

        
        XML_FOLDER = destination_dir
        XML_PARENT = destination_dir
        input_products_info_path =os.path.join(root_path, f"Input_products_info_text_files")
        INPUT_TEXT_PATH = os.path.join(input_products_info_path,f"S5-02-05_inputs_{REFERENCE_TIME}.txt")
        
        print("\nXML_FOLDER= "+ XML_FOLDER +"\nXML_PARENT = "+ XML_PARENT +"\nINPUT_TEXT_PATH = "+INPUT_TEXT_PATH)
        
        XML_CONFIGS["reference_time"] = REFERENCE_TIME
        XML_CONFIGS["xml_folder"] = XML_FOLDER
        XML_CONFIGS["xml_parent"] = XML_PARENT
        aoi = get_aoi_by_date(year, day, month)
        if aoi in [ConfigKeys.PO_BASIN.value, ConfigKeys.NORTH_ITALY.value, ConfigKeys.NORTH_SOUTH_ITALY.value, ConfigKeys.ITALY.value]:
            shapefile_path = XML_CONFIGS[ConfigKeys.SHAPEFILE_PATH.value][aoi]
            XML_CONFIGS[ConfigKeys.AOI.value] = aoi
        else:
            #shapefile_path = configs[ConfigKeys.SHAPEFILE_PATH.value]
            shapefile_path = XML_CONFIGS[ConfigKeys.SHAPEFILE_PATH.value]
        metadata_generation(
            xml_configs = XML_CONFIGS,
            input_text_path = INPUT_TEXT_PATH,
        )
