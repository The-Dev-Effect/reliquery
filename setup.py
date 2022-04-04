from setuptools import setup

install_requires = [
    "numpy >= 1.16",
    "black >= 21.4b",
    "pytest",
    "flake8",
    "sphinx",
    "pandas",
    "nbconvert",
    "nbformat",
    "Pillow >= 8.4.0"
]

setup(
    name="reliquery",
    version="0.3.2",
    description="Science's Artifact Antiformat",
    url="https://github.com/The-Dev-Effect/reliquery",
    author="The Dev Effect",
    author_email="company@thedeveffect.com",
    license="MIT OR Apache-2.0",
    packages=["reliquery"],
    tests_require=["pytest"],
    install_requires=install_requires,
    extras_require = {
        'S3' : ["boto3 >= 1.17"],
        'Dropbox' : ["dropbox"],
        'Google': ["google-api-python-client",
        "google-cloud-storage",
        "google-auth-httplib2",
        "google-auth-oauthlib"]
    }
)

