# delpip

Deleting PIPs entities concurrently

##
`conda create -n delpip python=3.5`

`source activate delpip`

`pip install requests`

`pip install aiohttp`

### Run
./delpip.py contributors

### sqlite3
.schema contributors
.mode csv
.load contributors.csv

