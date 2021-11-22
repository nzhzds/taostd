from setuptools import setup, find_packages

# INSTALL_PACKAGES = open(path.join(DIR, 'requirements.txt')).read().splitlines()

setup(
    name='taostd',
    packages=find_packages(),
    description="taostd is a simple sql executor for TDengine.",
    long_description_content_type='text/markdown',
    install_requires=[
        'cacheout>=0.13.1',
        'Click>=7.0',
        'taospy>=2.1.2',
        'typing>=3.7.4.3',
    ],
    version='0.0.1',
    url='https://gitee.com/summry/taostd/taostd',
    author='summry',
    author_email='xiazhongbiao@126.com',
    keywords=['sql', 'taos', 'TDengine', 'Time Series Database', 'python'],
    tests_require=[
        'pytest',
        'pandas'
    ],
    package_data={
        # include json and txt files
        '': ['*.rst', '*.txt'],
    },
    include_package_data=True,
    python_requires='>=3.6.10',
    zip_safe=False
)

