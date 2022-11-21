# Using this catalog

This guide assumes the following:

- JupyterHub is running
- The correct JupyterHub token is in the environment variable `JUPYTERHUB_API_TOKEN`
- The opal-catalog-be server is running on port 9001

To create a catalog run the following:

```python
from catalog_test import open_catalog
catalog = open_catalog("intake://localhost:9001")
```
