"""Helper function for translating an Oracle column type to Postgres

"""

from gobcore.exceptions import GOBException


def _oracle_number_to_postgres(length: int = None, precision: int = None, scale: int = None) -> str:
    """
    Returns the Postgres equivalent of the Oracle NUMBER column type for given length, precision and
    scale

    :param length:
    :param precision:
    :param scale:
    :return:
    """
    if precision is not None and scale is not None:
        return f'NUMERIC({precision},{scale})'

    if length is None:
        raise GOBException('Expect Oracle NUMBER to have either precision and scale or length')

    if length <= 4:
        return 'SMALLINT'
    elif length <= 9:
        return 'INT'
    elif length <= 18:
        return 'BIGINT'
    else:
        return f'NUMERIC({length})'


_oracle_postgresql_column_mapping = {
    'VARCHAR2': lambda l, p, s: f'VARCHAR({l})',
    'NARCHAR2': lambda l, p, s: f'VARCHAR({l})',
    'CHAR': lambda l, p, s: f'CHAR({l})',
    'NCHAR': lambda l, p, s: f'CHAR({l})',
    'DATE': lambda l, p, s: 'TIMESTAMP(0)',
    'TIMESTAMP WITH LOCAL TIME ZONE': lambda l, p, s: 'TIMESTAMPTZ',
    'CLOB': lambda l, p, s: 'TEXT',
    'NCLOB': lambda l, p, s: 'TEXT',
    'BLOB': lambda l, p, s: 'BYTEA',
    'NUMBER': _oracle_number_to_postgres,
    'SDO_GEOMETRY': lambda l, p, s: 'GEOMETRY',
}


def get_postgres_column_definition(data_type: str, length: int = None, precision: int = None,
                                   scale: int = None) -> str:
    """
    Returns the (string representation of the) Postgres equivalent of an Oracle column definition.

    For example:
    - given data_type VARCHAR2 and length 2, this function returns 'VARCHAR(2)'.
    - given data_type NUMBER, precision 2 and scale 1, this function returns 'NUMERIC(2,1)'

    :param data_type:
    :param length:
    :param precision:
    :param scale:
    :return:
    """
    try:
        return _oracle_postgresql_column_mapping[data_type](length, precision, scale)
    except KeyError:
        raise GOBException(f"Missing column type definition mapping for {data_type}")
