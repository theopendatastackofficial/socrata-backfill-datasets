descriptions_nyc_threeoneone_requests = {
    "unique_key": "Unique identifier of a Service Request (SR) in the open data set. [Text]",
    "created_date": "Date SR was created. [Floating Timestamp → Datetime]",
    "closed_date": "Date SR was closed by responding agency. [Floating Timestamp → Datetime]",
    "agency": "Acronym of responding City Government Agency. [Text]",
    "agency_name": "Full Agency name of responding City Government Agency. [Text]",
    "complaint_type": "The first level of a hierarchy identifying the topic of the incident or condition. May have a corresponding Descriptor or stand alone. [Text]",
    "descriptor": "Provides further detail on the incident or condition associated with the Complaint Type. [Text]",
    "location_type": "Describes the type of location used in the address information. [Text]",
    "incident_zip": "Incident location zip code, provided by geo validation. [Text]",
    "incident_address": "House number of incident address provided by the submitter. [Text]",
    "street_name": "Street name of incident address provided by the submitter. [Text]",
    "cross_street_1": "First Cross street based on the geo validated incident location. [Text]",
    "cross_street_2": "Second Cross Street based on the geo validated incident location. [Text]",
    "intersection_street_1": "First intersecting street based on geo validated incident location. [Text]",
    "intersection_street_2": "Second intersecting street based on geo validated incident location. [Text]",
    "address_type": "Type of incident location information available. [Text]",
    "city": "City of the incident location provided by geovalidation. [Text]",
    "landmark": "If the incident location is identified as a Landmark, the landmark's name will display here. [Text]",
    "facility_type": "If available, describes the type of city facility associated to the SR. [Text]",
    "status": "Status of the service request submitted. [Text]",
    "due_date": "Date when the responding agency is expected to update the SR based on Complaint Type and SLAs. [Floating Timestamp]",
    "resolution_description": "Describes the last action taken on the SR by the responding agency, potentially outlining next steps. [Text]",
    "resolution_action_updated_date": "Date when the responding agency last updated the SR. [Floating Timestamp]",
    "community_board": "Provided by geovalidation; identifies the community board overseeing the area. [Text]",
    "bbl": "Borough Block and Lot number (parcel number) provided by geovalidation, used to identify properties in NYC. [Text]",
    "borough": "The borough provided by the submitter and confirmed by geovalidation. [Text]",
    "x_coordinate_state_plane": "Geo validated X coordinate of the incident location. [Number]",
    "y_coordinate_state_plane": "Geo validated Y coordinate of the incident location. [Number]",
    "open_data_channel_type": "Indicates how the service request was submitted (e.g., Phone, Online, Mobile, Other, Unknown). [Text]",
    "park_facility_name": "If the incident location is a Parks Department facility, the facility's name will display here. [Text]",
    "park_borough": "Borough of the incident if it occurred at a Parks Department facility. [Text]",
    "vehicle_type": "If the incident is taxi-related, describes the type of TLC vehicle. [Text]",
    "taxi_company_borough": "If the incident is identified as a taxi, displays the borough of the taxi company. [Text]",
    "taxi_pick_up_location": "If the incident is taxi-related, displays the taxi pick-up location. [Text]",
    "bridge_highway_name": "If the incident is identified as a Bridge/Highway issue, the name of the bridge/highway will display here. [Text]",
    "bridge_highway_direction": "For Bridge/Highway incidents, displays the direction where the issue took place. [Text]",
    "road_ramp": "For Bridge/Highway incidents, differentiates if the issue occurred on the road or ramp. [Text]",
    "bridge_highway_segment": "Additional details on the section of the Bridge/Highway where the incident occurred. [Text]",
    "latitude": "Geo-based latitude of the incident location. [Number]",
    "longitude": "Geo-based longitude of the incident location. [Number]",
    "location": "Combination of the geo-based latitude & longitude of the incident location, often stored as a structured field. [Location]",
    "resolution_time_hours": "Time difference (in hours) between 'created_date' and 'closed_date'. Critical for assessing service response times. [Calculated Number]",
    "is_excel_date_error": "Flag that is set to 1 if 'closed_date' equals '1900-01-01', indicating an Excel default error; otherwise 0. [Error Flag]",
    "is_invalid_resolution_time": "Flag that is set to 1 if 'closed_date' exists but is earlier than 'created_date', indicating an invalid resolution time; otherwise 0. [Error Flag]",
    "cd_number": "Standardized numeric key extracted from 'community_board' based on an expected format (e.g., '07 BRONX' becomes 207 for Bronx). [Calculated Number]",
    "is_invalid_cd_number": "Flag that is set to 1 if 'cd_number' could not be computed (i.e. 'community_board' does not conform to the expected pattern), otherwise 0. [Error Flag]"
}



# Automatically generate table_descriptions
table_descriptions = {
    var_name.replace('descriptions_', ''): var_value
    for var_name, var_value in globals().items()
    if var_name.startswith('descriptions_')
}
