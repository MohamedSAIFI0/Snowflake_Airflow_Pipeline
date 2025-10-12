--Create a Stage for S3 Bucket
CREATE OR REPLACE STAGE my_s3_stage
URL = 's3://my-ecommerce-bucket/raw/'
STORAGE_INTEGRATION = my_integration;

--Create Snowpipes for Each Table
--Customer Pipe
CREATE OR REPLACE PIPE ecommerce_db.raw.customers_pipe
AS
COPY INTO ecommerce_db.raw.customers
FROM @my_s3_stage/customers/
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';



--Product Pipe
CREATE OR REPLACE PIPE ecommerce_db.raw.products_pipe
AS
COPY INTO ecommerce_db.raw.products
FROM @my_s3_stage/products/
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';


--Sales Pipe
CREATE OR REPLACE PIPE ecommerce_db.raw.sales_pipe
AS
COPY INTO ecommerce_db.raw.sales
FROM @my_s3_stage/sales/
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';


/*

Now you make Snowpipe automatic.

In AWS S3:

Go to your bucket → Properties → Event Notifications.

Create a new notification:

Event type: “All object create events”

Prefix: raw/customers/ (for the first pipe)

Destination: SNS topic linked to Snowflake.

Repeat for each folder:

raw/sales/

raw/products/

Each notification must send to the Snowflake-provided ARN from your storage integration.

*/



/*

If you just want to test loading manually (without waiting for S3 events):
ALTER PIPE ecommerce_db.raw.customers_pipe REFRESH;
ALTER PIPE ecommerce_db.raw.products_pipe REFRESH;
ALTER PIPE ecommerce_db.raw.sales_pipe REFRESH;
*/
