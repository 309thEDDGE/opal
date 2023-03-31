import uuid

from opal.weave.uploader import upload_basket

def upload(upload_items,
           bucket_name,
           basket_type,
           parent_ids = [],
           metadata = {},
           label = ''):
    """
    wrapper for upload_basket
    """
    #check data types for all parameters
    if not isinstance(upload_items, list):
        raise TypeError(f"'upload_items' must be a list of dictionaries: '{upload_items}'")
        
    if not all(isinstance(x, dict) for x in upload_items):
        raise TypeError(f"'upload_items' must be a list of dictionaries: '{upload_items}'")
        
    if not isinstance(bucket_name, str):
        raise TypeError(f"'bucket_name' must be a string: '{bucket_name}'")
        
    if not isinstance(basket_type, str):
        raise TypeError(f"'basket_type' must be a string: '{basket_type}'")

    if not isinstance(parent_ids, list):
        raise TypeError(f"'parent_ids' must be a list of int: '{parent_ids}'")

    if not all(isinstance(x, int) for x in parent_ids):
        raise TypeError(f"'parent_ids' must be a list of int: '{parent_ids}'")

    if not isinstance(metadata, dict):
        raise TypeError(f"'metadata' must be a dictionary: '{metadata}'")

    if not isinstance(label, str):
        raise TypeError(f"'label' must be a string: '{label}'")
    
    #generate unique id
    unique_id = uuid.uuid1().hex
    
    #build upload directory of the form
    # bucket_name/basket_type/unique_id/filename
    upload_directory = os.path.join(bucket_name, basket_type, unique_id)
    
    upload_basket(upload_items,
                 upload_directory,
                 unique_id,
                 basket_type,
                 parent_ids = [],
                 metadata = {},
                 label = '')
           