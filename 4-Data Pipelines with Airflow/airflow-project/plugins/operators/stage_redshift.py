from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format="",
                 json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.json_path = json_path


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Copying data from S3 to Redshift")
        s3key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{s3key}"

        if self.file_format == "json":
            command = f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' SECRET_ACCESS_KEY" \
                f" '{credentials.secret_key}' JSON '{self.json_path}'"
            redshift.run(command)
            
        if self.file_format == "csv":

            command = f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' " \
                f"SECRET_ACCESS_KEY '{credentials.secret_key}'" 
            redshift.run(command)





