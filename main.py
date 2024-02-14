# Custom Package's
from ETL.utils import Configuration
from ETL.create.CreateStageEnvironment import CreateStageEnvironment
from ETL.create.CreateTargetEnvironment import CreateTargetEnvironment
from ETL.create.CreateTransformationEnvironment import CreateTransformationEnvironment


if __name__ == "__main__":
    configuration = Configuration('./.config.ini')
    with configuration.get_session() as session:
        CreateStageEnvironment(configuration).generate(session, drop_table=True)
        CreateTargetEnvironment(configuration).generate(session, drop_table=True)
        CreateTransformationEnvironment(configuration).generate(session, drop_table=True)
