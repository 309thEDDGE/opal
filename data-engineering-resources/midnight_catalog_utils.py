import pandas as pd
   
# "un-melt" the dataframe to a 'wide' output
def to_dataframe(row_data, unstack=True):
    df = pd.DataFrame(row_data).drop_duplicates()
    if unstack:
        df = df.set_index(['_id', 'column']).unstack().droplevel(0, 1).reset_index(drop=True)
    return df

# really basic helper data structure
class MidnightCatalogGenerator():
    def __init__(self):
        self.rows = []
        
    def add(self, rows, entry_id):
        self.rows += [ r | { "_id" : entry_id } for r in rows ]
        
    def to_dataframe(self, **kwargs):
        return to_dataframe(self.rows, **kwargs)
    