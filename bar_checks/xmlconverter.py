from lxml import etree
import pandas as pd
from collections import Mapping
from commonlib import *


def df_to_xmletree(root_ele, mem_ele, df, index_name=None):
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)

    xml = etree.Element(str(root_ele))
    for i, row in df.iterrows():
        fields = {k: str(v) for k, v in row.items()}
        if index_name is not None:
            fields.update({index_name: str(i)})
        subelement = etree.Element(mem_ele, **fields)
        xml.append(subelement)

    return xml


def rcsv_addto_etree(value, root):
    if isinstance(root, str):
        root = etree.Element(root)
    elif not etree.iselement(root):
        raise TypeError('Invalid type of root: it must be either a string or etree.Element')

    if isinstance(value, Mapping):
        for fk, fv in value.items():
            if nontypes_iterable(fv):
                subelement = etree.Element(fk)
                root.append(rcsv_addto_etree(fv, subelement))
            else:
                root.set(fk, str(fv))
    elif isinstance(value, tuple) and len(value) == 2:
        subelement = etree.Element(value[0])
        root.append(rcsv_addto_etree(value[1], subelement))
    elif nontypes_iterable(value):
        for val in value:
            rcsv_addto_etree(val, root)
    else:
        root.text = str(value)

    return root


def to_xslstyle_xml(xml, xsl, stream):
    header = '<?xml version="1.0" encoding="utf-8"?>'
    xsl_ref = '<?xml-stylesheet type="text/xsl" href="{}"?>'.format(xsl)
    stream.write(header)
    stream.write(xsl_ref)
    text = etree.tostring(xml, pretty_print=True).decode('ascii')
    stream.write(text)
    # etree.ElementTree(xml).write(stream)

