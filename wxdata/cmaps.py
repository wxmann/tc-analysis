import os

from matplotlib import colors

from wxdata._colormodel import MAX_RGB_VALUE, to_rgba, to_fractional, rgb, rgba
from wxdata.workdir import savefile

__all__ = ['load_cmap']


def load_cmap(url):
    ext = os.path.splitext(url)[1]
    if not ext or ext != '.pal':
        raise ValueError("Cannot read file to cmap: {}".format(url))

    save_to_local = savefile(url, in_subdir='cmaps')
    if not save_to_local.success:
        raise ValueError("Cannot parse .pal file to cmap".format(url))

    return load_pal(save_to_local.dest)


def load_pal(file):
    name = os.path.basename(file).split('.')[0]
    rawcolors = parse_cmap_raw_colors(file)

    norm = colors.Normalize(min(rawcolors), max(rawcolors), clip=False)
    cmap_dict = colordict_to_cmap(rawcolors)
    return colors.LinearSegmentedColormap(name, cmap_dict), norm


#################################################
#    Converting .pal to dictionary of colors    #
#################################################


def parse_cmap_raw_colors(palfile):
    colorbar = {}

    with open(palfile, encoding='utf-8', errors='ignore') as paldata:
        for line in paldata:
            if line and line[0] != ';':
                results = _parse_pal_line(line)
                if isinstance(results, tuple) and len(results) == 2:
                    bndy, clrs = results
                    if bndy is not None:
                        colorbar[float(bndy)] = clrs

    return colorbar


def _parse_pal_line(line):
    tokens = line.split()
    header = tokens[0] if tokens else None

    if header is not None and 'color' in header.lower():
        def _getcolor(rgba_vals, has_alpha):
            if has_alpha:
                alpha = float(rgba_vals[3]) / MAX_RGB_VALUE
                return rgba(r=int(rgba_vals[0]), g=int(rgba_vals[1]), b=int(rgba_vals[2]), a=alpha)
            else:
                return rgb(r=int(rgba_vals[0]), g=int(rgba_vals[1]), b=int(rgba_vals[2]))

        cdata = tokens[1:]
        isrgba = 'color4' in header.lower()
        if not cdata:
            return None, None
        bndy = cdata[0]
        rgba_vals = cdata[1:]
        clrs = [_getcolor(rgba_vals, isrgba)]

        if len(rgba_vals) > 4:
            index = 4 if isrgba else 3
            rgba_vals = rgba_vals[index:]
            clrs.append(_getcolor(rgba_vals, isrgba))

        return bndy, clrs

    return None, None


###################################################
# Converting dict to cmap and norm for matplotlib #
###################################################


def colordict_to_cmap(colors_dict):
    cmap_dict = {
        'red': [],
        'green': [],
        'blue': [],
        'alpha': []
    }
    max_bound = max(colors_dict)
    min_bound = min(colors_dict)

    if max_bound == min_bound:
        raise ValueError("Color map requires more than one color")

    bounds_in_order = sorted(colors_dict.keys())

    for i, bound in enumerate(bounds_in_order):
        if i == len(bounds_in_order) - 1:
            # last element, avoid having extra entries in the colortable map
            pass
        else:
            lobound = bounds_in_order[i]
            hibound = bounds_in_order[i+1]
            locolors = colors_dict[lobound]
            hicolors = colors_dict[hibound]
            if not locolors or not hicolors:
                raise ValueError("Invalid colormap file, empty colors.")
            if len(locolors) < 2:
                locolors.append(hicolors[0])

            lobound_frac = (lobound - min_bound) / (max_bound - min_bound)
            hibound_frac = (hibound - min_bound) / (max_bound - min_bound)
            locolor1 = to_fractional(to_rgba(locolors[0]))
            locolor2 = to_fractional(to_rgba(locolors[1]))
            hicolor1 = to_fractional(to_rgba(hicolors[0]))

            def _append_colors(color):
                attr = color[0]
                # the first element
                if i == 0:
                    cmap_dict[color].append((lobound_frac, getattr(locolor1, attr), getattr(locolor1, attr)))
                cmap_dict[color].append((hibound_frac, getattr(locolor2, attr), getattr(hicolor1, attr)))

            _append_colors('red')
            _append_colors('green')
            _append_colors('blue')
            _append_colors('alpha')

    for k in cmap_dict:
        cmap_dict[k] = sorted(cmap_dict[k], key=lambda tup: tup[0])
    return cmap_dict
