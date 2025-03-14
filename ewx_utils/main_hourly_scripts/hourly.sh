!/bin/bash
# Defining stations array
stations=(
    aetna albion allegan alpine arlene arlington bainbridge bath
    bearlake belding benona benzonia berriensprings casco cassopolis ceresco 
    charlevoix charlotte chatham clarksville coldwater commercetwp conklin constantine
    deerfield demo dowagiac eastleland eastport eggharbor elbridge elkrapids emmett
    entrican escanaba fairgrove fennville flint freeland fremont gaylord goodells
    grandjunction grant hancock hart hartford haslett hastings hawks hudson 
    hudsonville ithaca kalamazoo kalkaska keeler kentcity kewadin kewaunee kinde 
    lancaster lapeer lawrence lawton leslie linwood ludington marshfield mcbain 
    mcmillan mears mecosta mendon mountpleasant msuhort msuhtrc msukbs munger 
    nasewaupee ncmc newera northport nwmhrs oldmission onekama oshtemo petersburg
    petoskey pigeon reeman rhinelander richville romeo saginaw sandusky scottdale 
    shelbyeast sisterbay southerndoor southhaven sparta20 sparta spartanorth spooner 
    standale stephenson sturgeonbay swmrec verona westjacksonport westolive williamsburg20 wmich
)
# Main processing loops
for station in "${stations[@]}"; do             # First loop: Station
    echo "Processing station $station..."
    for year in {1996..2005}; do               # Second loop: Years for current station
        echo "Processing year $year for station $station..."
        python3 hourly_main.py -x \
            -b "${year}-01-01" \
            -e "${year}-12-31" \
            -s "$station" \
            --read-from mawn rtma \
            --write-to mawnqc
    done
done

echo "All processing completed!"

