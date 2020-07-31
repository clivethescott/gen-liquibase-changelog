import re
from datetime import datetime, timedelta


AUTHOR = 'cgurure'
CATALOG = 'consultation'
SCHEMA = 'consultation'

col_regex = re.compile("(\w+) ([\w\(\)]+)")


class ColumnDef:
    def parse(self, data: str):
        if 'engine' in data or 'primary key' in data:
            return ''
        print("Processing parse with line {}".format(data))
        match = col_regex.search(data)
        self.col_name = match.group(1)
        self.col_type = match.group(2).upper()
        if self.col_type == 'DOUBLE':
            self.col_type = 'DOUBLE PRECISION'
        self.nullable = "false" if 'not null' in data else "true"
        self.parsed = True

    def __init__(self, table_name: str, sequence: str):
        self.table_name = table_name
        self.sequence = sequence
        self.parsed = False

    def to_add_column(self) -> str:
        if not self.parsed:
            return ''
        return f"""
            <column name="{self.col_name}" type="{self.col_type}">
                <constraints nullable="{self.nullable}"/>
            </column>
        """

    def to_changeset(self) -> str:
        if not self.parsed:
            return ''
        return f"""
        <changeSet id="{self.sequence}" author="{AUTHOR}" failOnError="true">
            <preConditions onFail="MARK_RAN">
                <not>
                    <columnExists catalogName="{CATALOG}" schemaName="{SCHEMA}"
                                  tableName="{self.table_name}" columnName="{self.col_name}"/>
                </not>
            </preConditions>
            <addColumn tableName="{self.table_name}" schemaName="{SCHEMA}" catalogName="{CATALOG}">
                {self.to_add_column()}
            </addColumn>
        </changeSet>
        """


alter_table_regex = re.compile(
    r"Hibernate: alter table consultation\.(\w+) add column (.*)")


def create_column_changeset(line: str, sequence: str) -> str:
    line = line.strip()
    match = alter_table_regex.search(line)
    table_name = match.group(1)
    column_data = match.group(2)
    column_def = ColumnDef(table_name, sequence)
    column_def.parse(column_data)
    return column_def.to_changeset()


create_table_regex = re.compile(
    r"Hibernate: create table consultation\.(\w+) \((.*)")


def create_table_changeset(line: str, sequence: str) -> str:
    line = line.strip()
    match = create_table_regex.search(line)
    table_name = match.group(1)
    columns = match.group(2).split(",")

    header = f"""
    <changeSet id="{sequence}" author="{AUTHOR}" failOnError="true">
        <preConditions onFail="MARK_RAN">
            <not>
                <tableExists catalogName="{CATALOG}" schemaName="{SCHEMA}"
                             tableName="{table_name}"/>
            </not>
        </preConditions>
        <createTable tableName="{table_name}" schemaName="{SCHEMA}" 
            catalogName="{CATALOG}">
    """

    footer = f"""
        </createTable>
        <addPrimaryKey catalogName="{CATALOG}"
                       columnNames="****" constraintName="PRIMARY"
                       schemaName="{SCHEMA}" tableName="{table_name}"/>
    </changeSet>
    """
    changeset = []
    changeset.append(header)
    for col in columns:
        col_def = ColumnDef(table_name, sequence)
        col_def.parse(col)
        changeset.append(col_def.to_add_column())
    return ''.join(changeset) + footer


DATE_FORMAT = "%Y-%m-%d %H%M"
PREV_CHANGELOG_ID = '2020-07-31 0945'


def format_date(date: datetime):
    return date.strftime(DATE_FORMAT)


def main():
    one_minute = timedelta(minutes=1)
    with open('input.txt', mode='r') as file:
        now = datetime.strptime(PREV_CHANGELOG_ID, DATE_FORMAT)
        change_sets = []
        for line in file:
            now += one_minute
            sequence = format_date(now)
            if 'create table' in line:
                change_sets.append(create_table_changeset(line, sequence))
            else:
                change_sets.append(create_column_changeset(line, sequence))
        with open('output.xml', mode='w') as output:
            output.write('\n'.join(change_sets))


if __name__ == "__main__":
    main()
