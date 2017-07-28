"""
This file brings together code from Dask Distributed Diagnostics
and Dask Distributed Bokeh. Along with the Dask Distributed Scheduler Plugin 
can be used to create post-mortem Dask Task Stream Bokeh plots

author: iparask
"""

import ast
from bokeh.palettes import viridis
from toolz import valmap, merge, memoize
import random
import itertools
import pandas as pd



task_stream_palette = list(viridis(25))
random.shuffle(task_stream_palette)

counter = itertools.count()

_incrementing_index_cache = dict()


@memoize(cache=_incrementing_index_cache)
def incrementing_index(o):
    return next(counter)


def color_of(o, palette=task_stream_palette):
    return palette[incrementing_index(o) % len(palette)]

def color_of_message(key):
        return color_of(key)

colors = {'transfer': 'red',
          'disk-write': 'orange',
          'disk-read': 'orange',
          'deserialize': 'gray',
          'compute': color_of_message}


alphas = {'transfer': 0.4,
          'compute': 1,
          'deserialize': 0.4,
          'disk-write': 0.4,
          'disk-read': 0.4}


prefix = {'transfer': 'transfer-',
          'disk-write': 'disk-write-',
          'disk-read': 'disk-read-',
          'deserialize': 'deserialize-',
          'compute': ''}


def key_split(s):
    
    if type(s) is bytes:
        s = s.decode()
    if type(s) is tuple:
        s = s[0]
    try:
        words = s.split('-')
        if not words[0][0].isalpha():
            result = words[0].lstrip("'(\"")
        else:
            result = words[0]
        for word in words[1:]:
            if word.isalpha() and not (len(word) == 8 and
                                       hex_pattern.match(word) is not None):
                result += '-' + word
            else:
                break
        if len(result) == 32 and re.match(r'[a-f0-9]{32}', result):
            return 'data'
        else:
            if result[0] == '<':
                result = result.strip('<>').split()[0].split('.')[-1]
            return result
    except:
        return 'Other'



def DaskProfileReader(filename):

    profFile = open(filename)
    profiles = profFile.readlines()
    buffer = [ast.literal_eval(x) for x in profiles]
    workIds = {}
    id = 1

    L_start = []
    L_duration = []
    L_key = []
    L_action = []
    L_name = []
    L_color = []
    L_alpha = []
    L_y = []

    for msg in buffer:
        key = msg[0]
        name = key_split(key)
        startstops = msg[-1]
        if not workIds.has_key(msg[3]):
            workIds[msg[3]]=id
            id+=1

        for action, start, stop in startstops:
            color = colors[action]
            if type(color) is not str:
                color = color(name)

            L_start.append((start + stop) / 2 * 1000)
            L_duration.append(1000 * (stop - start))
            L_key.append(key)
            L_action.append(action)
            L_name.append(name)
            L_color.append(color)
            L_alpha.append(alphas[action])
            L_y.append(workIds[msg[3]])


    return pd.DataFrame.from_dict({'start': L_start,
            'duration': L_duration,
            'key': L_key,
            'name': L_name,
            'action':L_action,
            'color': L_color,
            'alpha': L_alpha,
            'y': L_y})









