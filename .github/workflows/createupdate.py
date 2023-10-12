import sys

with open(sys.argv[1]) as view:
    view_str = view.read()
    if "CREATEORREPLACEVIEW" in view_str.upper().replace(' ', ''):
        pass
    else:
        raise Exception("Cannot find CREATE OR REPLACE VIEW in file.")