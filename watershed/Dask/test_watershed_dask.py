import os

from watershed_dask import watershed_multi

sizes = ['512KB', '1MB', '2MB', '4MB']
images_parent = './test_images'


def test_images_exist():
    images_paths = [os.path.join(images_parent, size) for size in sizes]
    for images_path in images_paths:
        assert os.path.exists(os.path.join(images_path, '0.jpg'))


def test_watershed_512KB(tmpdir):
    size = '512KB'
    input = size
    output = os.path.join(str(tmpdir), size)
    watershed_multi(images_parent, 0, 0, 0, '.jpg', input, output)
    assert os.path.exists(os.path.join(output, '0.jpg'))


def test_watershed_1MB(tmpdir):
    size = '1MB'
    input = size
    output = os.path.join(str(tmpdir), size)
    watershed_multi(images_parent, 0, 0, 0, '.jpg', input, output)
    assert os.path.exists(os.path.join(output, '0.jpg'))
