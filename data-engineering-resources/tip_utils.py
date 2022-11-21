import metaflow
import hashlib
import opal.flow
import pandas as pd
from pathlib import Path

# SHA-256 hash of the first 150MB of a chapter 10
def tip_hash_ch10(ch10_file):
    if isinstance(ch10_file, str) or isinstance(ch10_file, Path):
        with open(ch10_file, 'rb') as f:
            return tip_hash_ch10(f)
    else:
        
        BUF_SIZE = 65536
        HASH_SIZE = 150e6 #150MB

        sha = hashlib.sha256()

        hash_len = 0
        while True:
            # don't has more than HASH_SIZE bytes
            if HASH_SIZE - hash_len < BUF_SIZE:
                BUF_SIZE = int(HASH_SIZE - hash_len)
            data = ch10_file.read(BUF_SIZE)
            hash_len += len(data)

            if not data:
                # ran out of data in file
                break

            sha.update(data)
            if hash_len == HASH_SIZE:
                # hashed HASH_SIZE bytes -> we're done
                break

            if hash_len > HASH_SIZE:
                # this isn't supposed to happen
                raise Exception("Hashed more than 150MB. Something went wrong.")

        return sha.hexdigest()

# get the chapter 10 hash from the TIP metadata
def get_run_hash(parse_run):
    tip_meta = parse_run.data.tip_metadata
    any_key = list(tip_meta.keys())[0]
    resources = tip_meta[any_key]['provenance']['resource']
    
    ch10_hash = [ r['uid'] for r in resources if r['type'] == 'CH10' ]
    
    return ch10_hash[0]
        
def already_parsed_hashes():
    try:
        parse_flow = metaflow.Flow("TipParseFlow")
    except metaflow.exception.MetaflowNotFound:
        # the tip parse flow has not been ran before, so no data exists yet
        return {}
    
    return { get_run_hash(r) for r in parse_flow if r.successful and not "no_data" in r.tags }

def get_duplicate_parse_runs():
    # get a dataframe of metadata about the parse runs
    data = [ 
        {"id":r.id, "hash":get_run_hash(r), "create":r.created_at}
        for r in metaflow.Flow("TipParseFlow")
        if r.successful and not "no_data" in r.tags
    ]
    data_df = pd.DataFrame(data)
    
    # find the latest run for each unique hash
    latest_of_each_hashes = data_df.groupby("hash")['create'].max()
    latest_of_each = latest_of_each_hashes.to_frame().merge(data_df)

    # put together a list of runs to delete
    to_delete = []
    for row in latest_of_each.iloc:
        # delete a run if it has the same hash as a latest run,
        # and is not the latest run itself
        same_hash = data_df['hash'] == row['hash']
        not_same_run = data_df['id'] != row['id']
        others = data_df[same_hash & not_same_run]
        
        # make sure we aren't deleting anything newer than the latest run
        assert all(row['create'] > others['create'])
        to_delete.append(others)
        
    # return the ids
    return list(pd.concat(to_delete)['id'])

def get_orphan_translated_runs():
    valid_parse_runs = {
        r.id 
        for r in metaflow.Flow("TipParseFlow")
        if r.successful and not "no_data" in r.tags
    }
    
    # return all translate flow whose parse run ids are not in valid_parse_runs
    for run in metaflow.Flow("TipTranslateFlow"):
        if not run.successful or "no_data" in run.tags:
            continue
        if not run.data.parse_run.id in valid_parse_runs:
            yield run

if __name__ == "__main__":
    import sys
    if sys.argv[1] == "delete-parsed-duplicates":
        flow = metaflow.Flow("TipParseFlow")
        delete_ids = get_duplicate_parse_runs()
        for delete_id in delete_ids:
            run = flow[delete_id]
            print(f"Deleting {delete_id}")
            opal.flow.delete_run_data(run)
    elif sys.argv[1] == "delete-translated-orphans":
        for run in get_orphan_translated_runs():
            opal.flow.delete_run_data(run)
            print(f"Deleting {run.id}")
        