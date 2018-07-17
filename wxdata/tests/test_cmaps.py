from wxdata import cmaps
from wxdata.testing import resource_path


def test_create_cmap_from_rgb_pal():
    expected_cmap_dict = {
        'red': [(0.0, 145.0, 145.0), (0.25, 255.0, 255.0), (0.75, 31.0, 31.0),
                (1.0, 31.0, 31.0)],
        'green': [(0.0, 31.0, 31.0), (0.25, 255.0, 255.0), (0.75, 255.0, 255.0,), (1.0, 31.0, 31.0)],
        'blue': [(0.0, 145.0, 145.0), (0.25, 31.0, 31.0), (0.75, 255.0, 255.0),
                 (1.0, 31.0, 31.0)],
        'alpha': [(0.0, 1.0, 1.0), (0.25, 1.0, 1.0), (0.75, 1.0, 1.0), (1.0, 1.0, 1.0)]
    }

    cmap, norm = cmaps.load_pal(resource_path('pal_rgbtest.pal'))

    assert cmap._segmentdata == expected_cmap_dict

    assert norm.vmax == 100
    assert norm.vmin == -100
    assert norm.clip is False


def test_create_cmap_from_rgba_pal_with_discontinuties():
    expected_cmap_dict = {
        'red': [(0.0, 160.0, 160.0), (0.25, 250.0, 52.0), (0.5, 124.0, 0.0),
                (0.75, 0.0, 238.0), (1.0, 0.0, 0.0)],
        'green': [(0.0, 99.0, 99.0), (0.25, 0.0, 133.0), (0.5, 255.0, 4.0),
                  (0.75, 217.0, 243.0,), (1.0, 0.0, 0.0)],
        'blue': [(0.0, 45.0, 45.0), (0.25, 0.0, 15.0), (0.5, 0.0, 165.0),
                 (0.75, 255.0, 237.0), (1.0, 0.0, 0.0)],
        'alpha': [(0.0, 1.0, 1.0), (0.25, 1.0, 1.0), (0.5, 1.0, 1.0), (0.75, 1.0, 1.0), (1.0, 0.0, 0.0)]
    }

    cmap, norm = cmaps.load_pal(resource_path('pal_rgbatest.pal'))

    assert cmap._segmentdata == expected_cmap_dict

    assert norm.vmax == 40
    assert norm.vmin == 0
    assert norm.clip is False
