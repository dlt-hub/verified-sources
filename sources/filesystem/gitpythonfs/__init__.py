# file exists to make pytest work.
#
# pytest (or maybe just pytest discovery) expects all nested folders to be packages.
#
# This __init__.py file not required once gitpythonfs is a completely separate
# package to dlt-verified-sources or it's tests are moved to /tests.
#
# An alternative is to run pytest specifically on a subfolder, e.g.:
#     pytest sources/filesystem/gitpythonfs
