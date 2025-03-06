#libraries
library(utils)
library(readr)
library(dplyr)
library(sf)

#working directory
setwd("Z:/2459 Plan BTV NNE/70 Data/01_Analysis/ParcelData")

#01 LOAD DATA + LIGHT DATA CLEANING

#original Assessor's Property Information
assessors_raw <- read_csv("01_Original/Assessor_Property_Information_edited.csv")

#parcel data
parceldata <- st_read("01_Original/ParcelData_fromCity/ParcelData.shp")
parceldata$LOTNO_SHORT <- substr(parceldata$MAPLOTNO, 1, 9)

# nne boundary
nne_boundary <- st_read("01_Original/NNE Boundary_rev/NNE Boundary_rev.shp")

# #check for parcel duplicates ----
# parceldata_unique <- parceldata %>% distinct(.keep_all = TRUE)
# parceldata_dup <- parceldata %>%
#   group_by(MAPLOTNO) %>%
#   filter(n() > 1) %>%
#   ungroup()

#confirm that all entries have a neighborhood associated with them ----
sum(is.na(assessors_raw$NBRHD) | assessors_raw$NBRHD == "")
assessors_raw_no_nbhrd <- assessors_raw[assessors_raw$NBRHD == "", ]
assessors_raw_na_nbhrd <- assessors_raw[is.na(assessors_raw$NBRHD), ]


#02 FILTERING: NNE

#filter for assessors data in study area - NNE, INTERVALE, WATERFRONT ----
assessors_raw_nne <- assessors_raw[grepl("new north end|NNE|INTERVALE|WATERFRONT|610|Starr Farm Beach|010", assessors_raw$NBRHD, ignore.case = TRUE), ]

#filter for parcels in study area
nne_boundary_crs <- st_transform(nne_boundary, st_crs(parceldata))
parceldata_nne_indices <- st_intersects(parceldata, nne_boundary_crs, sparse = FALSE)
parceldata_nne <- parceldata[rowSums(parceldata_nne_indices) > 0, ]

#03 ANALYSIS OF DUPLICATES WITHIN ASSESSORS PROPERTY DATA
#extract assessors data that has a duplicate ID_SHORT. 
#note that this was pre-done outside of this script
assessors_dup_with <- assessors_raw[assessors_raw$IS_DUP == TRUE, ]
#assessors_nne_dup_with <- assessors_raw_nne[assessors_raw_nne$IS_DUP == TRUE, ]

# keep duplicates if 1) all parcels are vacant, 2) all parcels are NOT vacant. remove duplicates where at least one, but not none, of dups are nonvacant.
assessors_dup_kept <- assessors_dup_with %>%
  group_by(ID_SHORT) %>%
  mutate(all_vacant = all(VACANCY == "Vacant - Vacant"),
         all_non_vacant = all(VACANCY != "Vacant - Vacant")) %>%
  filter(all_vacant | all_non_vacant | VACANCY != "Vacant - Vacant") %>%
  select(-all_vacant, -all_non_vacant) %>%
  ungroup()

# parcels removed - remove these from the original dataset
#so, assessors_dup_kept + assessors_dup_removed = all duplicates
# assessors_unique1 + assessors_dup_removed = entire assessors dataset
assessors_dup_removed <- anti_join(assessors_dup_with, assessors_dup_kept, by = c("OBJ_ID"))
assessors_unique1 <- anti_join(assessors_raw, assessors_dup_removed, by = c("OBJ_ID"))

# confirm no duplicate PARCEL_ID ----
assessors_unique1_dup <- assessors_unique1 %>%
  group_by(PARCEL_ID) %>%
  filter(n() > 1) %>%
  ungroup()


#04 SPATIAL JOINS ----
# join 1: join by PARCEL_ID ----
join1_parcels_matched <- parceldata_nne %>%
  inner_join(assessors_unique1, by = c("MAPLOTNO" = "PARCEL_ID"))

join1_parcels_unmatched <- anti_join(parceldata_nne, assessors_unique1, by = c("MAPLOTNO" = "PARCEL_ID"))

join1_assessors_unmatched <- anti_join(assessors_unique1, parceldata_nne, by = c("PARCEL_ID" = "MAPLOTNO"))

# join 2: join by ID_SHORT, but only if there is exactly one match ----
#assessors_unique2 is data that failed to join during join 1 (since the long id didn't match), but only one instance of its ID_SHORT so it's safe to join by ID_SHORT
assessors_unique2 <- join1_assessors_unmatched  %>%
  group_by(ID_SHORT) %>%
  filter(n() == 1) %>%
  ungroup()

#assessors_dup2 = where there's a duplicate in ID_SHORT
# assessors_unique2 + assessors_dup2 = join1_assessors_unmatched
assessors_dup2 <- join1_assessors_unmatched  %>%
  group_by(ID_SHORT) %>%
  filter(n() > 1) %>%
  ungroup()

#parcels that successfully join by ID_SHORT
join2_parcels_matched <- join1_parcels_unmatched %>%
  inner_join(assessors_unique2, by = c("LOTNO_SHORT" = "ID_SHORT"))

#parcels that weren't joined - 0 matches with ID_SHORT or multiple matches
#at this point, if there are zero matches with ID_SHORT, the parcel is "unjoinable" and will need to manually fill in data
join2_parcels_unmatched <- anti_join(join1_parcels_unmatched, assessors_unique2, by = c("LOTNO_SHORT" = "ID_SHORT"))

#remaining assessors data
join2_assessors_unmatched <- bind_rows(anti_join(assessors_unique2, join1_parcels_unmatched, by = c("ID_SHORT" = "LOTNO_SHORT")), assessors_dup2)

# aggregate 1: for remaining unmatched assessor's data, it's bc it's either not in the NNE, OR, ID_SHORT is not unique. So, need to aggreagte by ID-short
mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

# custom Mode function
Mode <- function(x) {
  x <- x[!is.na(x) & x != ""]  # Remove NA and empty values
  
  if (length(x) == 0) return(NA)  # Return NA if no valid values exist
  
  unique_x <- unique(x)  # Get unique values
  mode_value <- unique_x[which.max(tabulate(match(x, unique_x)))]
  
  return(mode_value)
}

#define column functions ----
funcs <- c(mean, sum, first, mode, max)  # Define your functions

column_funcs <- c("OBJ_ID" = first, 
"PIN" = first, 
"PROP_ID" = first,
"XCOORD" = first, 
"YCOORD" = first, 
"PARCEL_ID" = first, 
"ADDRESS" = first, 
"ZIPCODE" = first, 
"LANDUSE" = mode,
"NBRHD" = first,
"VACANCY" = mode, 
"A_PARCEL" = max,
"A_FINISH" = sum, 
"V_LAND" = sum, 
"V_YARD" = sum, 
"V_BUILD" = sum, 
"V_TOT" = sum,
"V_ASSESS" = sum, 
"DEPREC" = mean, 
"ZONING" = mode, 
"BLDGTYP" = mode,
"CONDITION" = mode, 
"QUAL_DESC" = mode, 
"UNITS_C" = sum, 
"UNITS_R" = sum, 
"STORIES" = mode, 
"FOUND" = mode, 
"FRAME" = mode, 
"ROOF_STR" = mode,
"ROOF_COV" = mode, 
"WALL_INT" = mode, 
"WALL_INT1" = mode, 
"WALL_EXT" = mode, 
"WALL_EXT1" = mode, 
"BASEMENT" = mode, 
"PARKING" = mode, 
"ELECTRIC" = mode, 
"INSUL" = mode, 
"PLUMB" = mode, 
"FUEL" = mode, 
"HEAT" = mode, 
"HEAT2" = mode, 
"PERC_HEAT" = mean, 
"PERC_AIR" = mean, 
"YEARBUILT" = min, 
"BEDS" = sum, 
"BATHS" = sum, 
"OWN_NAME" = mode, 
"OWN_ADD" = mode, 
"OWN_ADD1" = mode, 
"OWN_CITY" = mode, 
"OWN_STATE" = mode, 
"OWN_STATE_1" = mode, 
"DATADATE" = first, 
"IS_DUP" = first)

#aggregation function ----
assessors_unique3 <- join2_assessors_unmatched %>%
  group_by(ID_SHORT) %>%
  summarise(across(names(column_funcs), ~ {
    func <- column_funcs[[cur_column()]]
    if ("na.rm" %in% names(formals(func))) {
      func(.x, na.rm = TRUE)  # Only pass na.rm if the function supports it
    } else {
      func(.x)  # Call function without na.rm if it's not supported
    }
  }
  ))


# confirm no duplicate ID_SHORT ----
aggregate_dup <- join2_assessors_unmatched_unique %>%
  group_by(ID_SHORT) %>%
  filter(n() > 1) %>%
  ungroup()

#join 3 - same join as join 2 but using join3_assessors


# join 3:
#now that there must be only 1 occurence of ID_SHORT, safe to join by ID_SHORT again
join3_parcels_matched <- join2_parcels_unmatched %>%
  inner_join(assessors_unique3, by = c("LOTNO_SHORT" = "ID_SHORT"))

join3_parcels_unmatched <- anti_join(join2_parcels_unmatched, assessors_unique3, by = c("LOTNO_SHORT" = "ID_SHORT"))
true_unmatched <- join3_parcels_unmatched %>%
  filter(!MAPLOTNO %in% c('000-0-000-000', '888-8-888-888'))

join3_assessors_unmatched <- anti_join(join2_assessors_unmatched, join2_parcels_unmatched, by = c("ID_SHORT" = "LOTNO_SHORT"))

# put the joins together
final_join <- bind_rows(join1_parcels_matched, join2_parcels_matched, join3_parcels_matched, join3_parcels_unmatched)

#### hidden code ----
# join1_assessors <- assessors_dup_kept %>%
#   left_join(parceldata, by = c("PARCEL_ID" = "MAPLOTNO"))

# join1_assessors_dup <- join1_assessors %>%
#   group_by(PARCEL_ID) %>%
#   filter(n() > 1) %>%
#   ungroup()

# plot and export ----
plot(unknown$geometry, col = "lightblue", border = "darkblue", main = "Shapefile Visualization")
plot(final_join$geometry)
st_write(final_join, "03_Final/ParcelData_join_Assessor/ParcelData_join_Assessor.shp", delete_layer = TRUE) 

# join 2: join by short_ID, but only if there is exactly one match
join2_parcels <- 

# join 3: identify data that didn't get matched, then aggregate...


# attempt to join the duplicates only with the actual shapes. if there is exactly one match, join. if zero or multiple matches, do not join


# remove parcels removed from the original dataset

#attempt a join 



#for all duplicates, (maybe iterate through)
#use the layer 

#aggregate land values 
