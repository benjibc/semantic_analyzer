import re

def remove_quotes(text):
    p = re.compile(r'\"(.+?)\"', re.DOTALL)
    return p.sub('', text)

def remove_single_quotes(text):
    p = re.compile(r'\'(.+?)\'', re.DOTALL)
    return p.sub('', text)


def remove_bracket(text):
    p = re.compile(r'\((.+?)\)', re.DOTALL)
    return p.sub('', text)
