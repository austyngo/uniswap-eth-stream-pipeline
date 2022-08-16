import setuptools

REQUIRED_PACKAGES = [
    'google-cloud-storage',
    'fastavro'
]

setuptools.setup(
    name='stream-transform',
    version = '0.0.0',
    setup_requires = REQUIRED_PACKAGES,
    install_requires = REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    include_package_data=True
)