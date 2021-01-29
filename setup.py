from setuptools import setup, find_packages

requires = [
    'flask',
    'psycopg2',
]

# TODO
setup(
    name='skynet',
    version='0.0',
    description='RevenYou BI system',
    author='Hamed Babaeian',
    author_email='hamed@revenyou.io',
    keywords='skynet',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requires
)
