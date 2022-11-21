import opal.query
import opal.publish
import metaflow
import opal.flow

def regenerate_catalog(publish_all, publish_latest):
    # get everything in the catalog
    search = opal.query.search.Instance()
    res = search.search()
    catalog = [ d["kind_id"] for d in res.all() ]
    
    # delete it all
    print("Deleting old catalog...")
    n = len(catalog)
    for i, c_id in enumerate(catalog):
        opal.publish.delete(c_id)
        print(f"\r{i+1}/{n} : {c_id}", end="")
        
    # publish all valid runs for every flow name in publish_all
    print("\nPublishing new catalog...")
    for flow in publish_all:
        print(f"\tall: {flow.id}")
        for run in flow:
            opal.flow.publish_run(run)

    # publish the latest successful run for every flow in publish_latest
    for flow in publish_latest:
        print(f"\tlatest: {flow.id}")
        opal.flow.publish_run(flow.latest_successful_run)
        
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser("Regenerate the OPAL catalog")
    parser.add_argument(
        "--all", metavar="FLOW_NAME", nargs="*", 
        help="publish all runs for these flows", default=[]
    )
    parser.add_argument(
        "--latest", metavar="FLOW_NAME", nargs="*", 
        help="publish only the latest successful run for these flows", default=[]
    )
    
    args = parser.parse_args()
    
    # get the flows now so we don't encounter an exception
    # halfway through regenerating the catalog
    publish_all_flows = [ metaflow.Flow(fl_id) for fl_id in args.all ]
    publish_latest_flows = [ metaflow.Flow(fl_id) for fl_id in args.latest ]
    
    regenerate_catalog(publish_all_flows, publish_latest_flows)
    