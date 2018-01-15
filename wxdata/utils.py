import matplotlib.colors as colors
import matplotlib.cm as cm


def diff(df1, df2):
    return df1.merge(df2, indicator=True, how='outer')


def sample_colors(n, src_cmap):
    return _ColorSamples(n, src_cmap)


class _ColorSamples(object):
    def __init__(self, n_samples, cmap):
        self.n_samples = n_samples
        color_norm = colors.Normalize(vmin=0, vmax=self.n_samples - 1)
        self.scalar_map = cm.ScalarMappable(norm=color_norm, cmap=cmap)

    def __getitem__(self, item):
        return self.scalar_map.to_rgba(item)

    def __iter__(self):
        for i in range(self.n_samples):
            yield self.scalar_map.to_rgba(i)

    def __len__(self):
        return self.n_samples