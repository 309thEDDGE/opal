cd $(dirname $0)/..

# generate the chapter 10 catalog
python chapter_10_catalog.py run \
--sources acceptance-environment/acceptance_ch10_sources.json

# parse it all
python tip_parse_flow_wrapper.py

# clean out the parse and translate data
python tip_utils.py delete-parsed-duplicates
python tip_utils.py delete-translated-orphans

# translate it all
python tip_translate_flow_wrapper.py MILSTD1553 ~/DTS/DTS1553_Synth_Nasa.yaml
# no arinc429 data for now
# python tip_translate_flow_wrapper.py ARINC429 ~/DTS/DTS429_Synth_NASA.yaml

# re-catalog everything
python tip_midnight_catalog_generator.py run
python regenerate_catalog.py --all TipParseFlow TipTranslateFlow --latest TipMidnightCatalog Chapter10Catalog