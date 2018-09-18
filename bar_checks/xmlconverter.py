from lxml import etree
from collections import Mapping
from commonlib import *


def df_to_xmletree(root, sub_name, df, index_name=None):
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)

    xml = root
    if isinstance(root, str):
        xml = etree.Element(root)
    elif not etree.iselement(root):
        raise TypeError('Invalid type of root: it must be either a string or etree.Element')

    for i, row in df.iterrows():
        subelement = etree.Element(sub_name)
        if index_name is not None:
            subelement.set(index_name, str(i))
        for k, v in row.items():
            subelement.set(k, str(v))

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
                root.append(rcsv_addto_etree(fv, fk))
            else:
                root.set(fk, str(fv))
    elif isinstance(value, tuple) and len(value) == 2:
        root.append(rcsv_addto_etree(value[1], value[0]))
    elif nontypes_iterable(value):
        for val in value:
            rcsv_addto_etree(val, root)
    else:
        root.text = str(value)

    return root


def to_xsl_instructed_xml(xml, xsl, outpath, encoding='utf-8'):
    header = u'<?xml version="1.0" encoding="{}"?>\n'.format(encoding)
    xsl_pi = u'<?xml-stylesheet type="text/xsl" href="{}"?>\n'.format(xsl)

    with open(outpath, 'w+', encoding=encoding) as stream:
        stream.write(header)
        stream.write(xsl_pi)
        text = etree.tostring(xml, pretty_print=True).decode(encoding)
        stream.write(text)
    return xml
    # etree.ElementTree(xml).write(stream)


def to_styled_xml(xml, xsl, outpath=None, encoding='utf-8'):
    dom = xml
    if not etree.iselement(dom):
        dom = etree.parse(xml)
    transform = etree.XSLT(etree.parse(xsl))
    newdom = transform(dom)
    if outpath is not None:
        with open(outpath, mode='w+') as stream:
            text = etree.tostring(newdom, pretty_print=True).decode(encoding)
            stream.write(text)
    return newdom
