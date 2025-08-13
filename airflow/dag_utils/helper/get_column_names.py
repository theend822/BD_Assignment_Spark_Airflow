"""
Utility functions to generate column lists for DQ checks based on BD transformer patterns.
"""

def get_columns_from_config(config_path, prefix_list=None, suffix_list=None):
    """
    Extract column names from BD transformer YAML config with optional prefix and suffix list.
    
    Args:
        config_path (str): Path to the transformer config file
        prefix (list): Prefix to add to each column name, needs to have underscore at the end, example: ['pre_', 'post_']
        suffix_list (list): List of suffixes to add to each column name, needs to have underscore at the start, example: ['_data', '_valid', '_error']
    
    Returns:
        list: Column names with applied prefix/suffixes
    """
    import yaml
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Column names are the top-level keys in the config
    base_columns = list(config.keys())

    # Default to empty string so combinations still work
    prefix_list = prefix_list if prefix_list else [""]
    suffix_list = suffix_list if suffix_list else [""]

    # Generate all combinations
    final_columns = [
        f"{prefix}{col}{suffix}"
        for col in base_columns
        for prefix in prefix_list
        for suffix in suffix_list
    ]

    return final_columns