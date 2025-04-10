import pandas as pd
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    DatasetSnapshotClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    DatasetPropertiesClass,
)
from datahub.emitter.mce_builder import make_dataset_urn

# Step 1: Load the Parquet file
file_path = "/Users/dkshirsagar/Documents/DatahubDemo/yellow_tripdata_2025-01.parquet"
df = pd.read_parquet(file_path)

# Step 2: Define dataset URN
platform = "parquet"
dataset_name = "yellow_tripdata_2025_01"
env = "DEV"
dataset_urn = make_dataset_urn(platform, dataset_name, env)

# Step 3: Build schema fields
fields = []
for col in df.columns:
    schema_field = SchemaFieldClass(
        fieldPath=col,
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),  # You can improve this with type inference
        nativeDataType=str(df[col].dtype),
        description=f"Auto-generated field for column: {col}"
    )
    fields.append(schema_field)

# Step 4: Create MetadataChangeEvent
mce = MetadataChangeEventClass(
    proposedSnapshot=DatasetSnapshotClass(
        urn=dataset_urn,
        aspects=[
            DatasetPropertiesClass(description="Ingested from Parquet using Python script."),
            SchemaMetadataClass(
                schemaName=dataset_name,
                platform="urn:li:dataPlatform:parquet",
                version=0,
                hash="",  # can be empty if not versioning
                platformSchema={"com.linkedin.structured.StructuredSchemaMetadata": {}},
                fields=fields
            )
        ]
    )
)

# Step 5: Emit to DataHub
emitter = DatahubRestEmitter(gms_server="http://localhost:8080")
emitter.emit(mce)

print(f"✅ Metadata for {dataset_name} successfully emitted to DataHub!")
