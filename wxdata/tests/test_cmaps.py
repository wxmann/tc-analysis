from wxdata import cmaps
from wxdata.testing import resource_path


def test_create_cmap_from_rgb_pal():
    expected_cmap_dict = {
        'red': [(0.0, 145 / 255., 145 / 255.), (0.25, 1.0, 1.0), (0.75, 31 / 255., 31 / 255.),
                (1.0, 31 / 255., 31 / 255.)],
        'green': [(0.0, 31 / 255., 31 / 255.), (0.25, 1.0, 1.0), (0.75, 1.0, 1.0,), (1.0, 31 / 255., 31 / 255.)],
        'blue': [(0.0, 145 / 255., 145 / 255.), (0.25, 31 / 255., 31 / 255.), (0.75, 1.0, 1.0),
                 (1.0, 31 / 255., 31 / 255.)],
        'alpha': [(0.0, 1.0, 1.0), (0.25, 1.0, 1.0), (0.75, 1.0, 1.0), (1.0, 1.0, 1.0)]
    }

    cmap, norm = cmaps.load_pal(resource_path('pal_rgbtest.pal'))

    assert cmap._segmentdata == expected_cmap_dict

    assert norm.vmax == 100
    assert norm.vmin == -100
    assert norm.clip is False


def test_create_cmap_from_rgba_pal_with_discontinuties():
    expected_cmap_dict = {
        'red': [(0.0, 160 / 255., 160 / 255.), (0.25, 250 / 255., 52 / 255.), (0.5, 124 / 255, 0),
                (0.75, 0, 238 / 255), (1.0, 0, 0)],
        'green': [(0.0, 99 / 255., 99 / 255), (0.25, 0, 133 / 255), (0.5, 1, 4 / 255),
                  (0.75, 217 / 255., 243 / 255.,), (1.0, 0, 0)],
        'blue': [(0.0, 45 / 255., 45 / 255), (0.25, 0, 15 / 255), (0.5, 0, 165 / 255),
                 (0.75, 1, 237 / 255), (1.0, 0, 0)],
        'alpha': [(0.0, 1.0, 1.0), (0.25, 1.0, 1.0), (0.5, 1.0, 1.0), (0.75, 1.0, 1.0), (1.0, 0.0, 0.0)]
    }

    cmap, norm = cmaps.load_pal(resource_path('pal_rgbatest.pal'))

    assert cmap._segmentdata == expected_cmap_dict

    assert norm.vmax == 40
    assert norm.vmin == 0
    assert norm.clip is False
