import metaflow

import tip_translate_flow
import os
import subprocess
import sys

# run the tip translate flow for a parse dataset with a dts file
def translate_one(parse_id, dts, translate_type):
    subprocess.run(
        [
            "python",
            "tip_translate_flow.py",
            "--no-pylint",
            "run",
            "--parsed",
            parse_id,
            "--dts",
            dts,
            "--type",
            translate_type,
        ]
    )


# We want to translate a parsed run when:
# 1. The parsed run is successful and has data
# 2. The parsed run doesn't already have a translated run of this type with this DTS
# Some cleanup will have to happen later
def filter_ran(parse_inputs, dts, translate_type):
    parse_inputs = set(parse_inputs)

    try:
        translate_flow = metaflow.Flow("TipTranslateFlow")
    except metaflow.exception.MetaflowNotFound:
        # no translated data yet? Just return everything
        return parse_inputs

    # parse datasets that have already been translated
    already_translated = {
        r.data.parse_pointer
        for r in translate_flow
        if r.successful
        and r.data.dts == dts
        and r.data.translate_type == translate_type
    }

    parse_flow = metaflow.Flow("TipParseFlow")

    # parsed datasets that shouldn't be translated for whatever reason
    invalid_parse_sets = {
        r
        for r in parse_inputs
        if "no_data" in parse_flow[r].tags or not parse_flow[r].successful
    }

    # parsed datasets that don't have any data of the type to be translated
    # i.e. we're translating ARINC429, but the parsed data only has MILSTD1553
    _, parse_type = tip_translate_flow.translate_options[translate_type]
    no_data_of_type = {
        r for r in parse_inputs
        if (
            parse_flow[r].successful and 
            not f"parsed_{parse_type}" in parse_flow[r].data.tip_metadata
        )
    }
    
    # let the user know what's going on
    if already_translated:
        print(
            "These parsed datasets have already been translated with this DTS file and will not be translated again:"
        )
        for pid in parse_inputs.intersection(already_translated):
            print(pid)

    if no_data_of_type:
        print(f"These parsed datasets don't have any {parse_type} data:")
        for pid in parse_inputs.intersection(no_data_of_type):
            print(pid)

    return parse_inputs - already_translated - invalid_parse_sets - no_data_of_type


def main(dts, translate_type, force=False):
    parse_flow = metaflow.Flow("TipParseFlow")
    inputs = [r.id for r in parse_flow]
    good_inputs = inputs if force else filter_ran(inputs, dts, translate_type)

    n = len(good_inputs)
    for i, parse_input in enumerate(good_inputs):
        print(f"{i}/{n}")
        translate_one(parse_input, dts, translate_type)


if __name__ == "__main__":
    if not sys.argv[1] in tip_translate_flow.translate_options:
        print(f"Not a translatable type: {sys.argv[1]}")
        print(f"Choose from one of {tip_translate_flow.translate_options.keys()}")
    main(sys.argv[2], sys.argv[1])

