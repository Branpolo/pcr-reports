# Resolution Codes and LIMS Status Analysis

## A. UNIQUE RESOLUTION CODES (for 9,488 resolved non-discrepancy errors)

Total: **23 unique codes** with these patterns:

### Primary Codes:
- **SKIP** - 8,232 occurrences (86.7%) - Main resolution action, means sample was repeated
- **BLA** - 39 occurrences - Baseline adjustment (but these shouldn't be here after filtering?)
- **BPEC** - 2 occurrences - Unknown meaning

### Secondary Codes (combined with SKIP):
- **WDCT** - 73 occurrences - Likely "Wrong Delta CT" or CT-related issue
- **WG14S, WG13S, WG7T, WG12S, WG22S** - Various WG (well group?) codes with numbers
- **RPA, RPP, RPN** - Repeat codes (Repeat Positive/Amplification/Negative?)
- **RXA, RXP, RXN** - Re-extract codes (Re-extract Amplification/Positive/Negative?)
- **WDCTC** - 1 occurrence - CT control variant

### Common Combinations:
1. `SKIP` alone - 8,232 (vast majority)
2. `SKIP,WDCT` - 73 
3. `SKIP,WG14S` - 25
4. `SKIP,WG13S` - 17
5. `SKIP,RPA,WG14S` - 14

## B. UNIQUE LIMS EXPORT STATUSES (for resolved errors)

Total: **10 unique statuses**

1. **EXCLUDE** - 5,145 (54.2%) - Sample excluded from results
2. **DETECTED** - 1,220 (12.9%) - Pathogen detected
3. **REXCT** - 864 (9.1%) - Re-extraction
4. **NOT DETECTED** - 679 (7.2%) - Pathogen not detected  
5. **INCONCLUSIVE** - 90 - Cannot determine result
6. **REAMP** - 67 - Re-amplification
7. **(NULL/Empty)** - 66 - No LIMS status
8. **TNP** - 30 - Test Not Performed?
9. **RXT** - 7 - Re-extract (variant)
10. **RPT** - 3 - Repeat

## Key Insights for Report Optimization:

1. **SKIP dominates** - 86.7% of resolved errors just have "SKIP" as resolution
2. **EXCLUDE is most common LIMS status** - 54% of resolved errors are excluded
3. **Binary outcomes exist** - DETECTED (12.9%) and NOT DETECTED (7.2%) for resolved errors that weren't excluded
4. **Re-processing common** - REXCT (864), REAMP (67), RXT (7), RPT (3) = 941 total re-processed samples

## For Discrepancy Wells (reference):
- **BLA** is most common (13) - Makes sense for baseline adjustments
- Various classification-specific codes like WDCLS, SETPOS