import os
import json
import time
import webbrowser
from datetime import datetime
from fastavro import reader
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    NumberTypeClass,
    BooleanTypeClass,
    AuditStampClass,
    OtherSchemaClass,
)
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.rest_emitter import DatahubRestEmitter

# CONFIG 
DATAHUB_GMS_SERVER = "http://localhost:8080"
PLATFORM = "avro"
ENV = "DEV"
SCHEMA_CACHE_DIR = "./avro_schema_cache"
REPORT_DIR = "./avro_reports"

AVRO_FILES_INFO = [
    {"file_path": "/Users/dkshirsagar/Documents/DatahubDemo/userdata1.avro", "dataset_name": "userdata1"},
    {"file_path": "/Users/dkshirsagar/Documents/DatahubDemo/userdata2.avro", "dataset_name": "userdata2"},
    {"file_path": "/Users/dkshirsagar/Documents/DatahubDemo/userdata3.avro", "dataset_name": "userdata3"},
    {"file_path": "/Users/dkshirsagar/Documents/DatahubDemo/userdata4.avro", "dataset_name": "userdata4"},
    {"file_path": "/Users/dkshirsagar/Documents/DatahubDemo/userdata5.avro", "dataset_name": "userdata5"},
]

os.makedirs(SCHEMA_CACHE_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

def generate_html_diff_report(dataset_name, old_fields, new_fields):
    report_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_path = f"{REPORT_DIR}/schema_diff_{dataset_name}_{report_time}.html"

    added = [f for f in new_fields if f not in old_fields]
    removed = [f for f in old_fields if f not in new_fields]
    changed_types = [
        f for f in new_fields if f in old_fields and new_fields[f] != old_fields[f]
    ]

    with open(report_path, "w") as f:
        f.write("<html><head><title>Schema Diff Report</title></head><body>")
        f.write(f"<h2>Schema Diff Report for <code>{dataset_name}</code></h2>")

        if added:
            f.write("<h3 style='color:green;'>➕ Added Fields:</h3><ul>")
            for field in added:
                f.write(f"<li>{field}: {new_fields[field]}</li>")
            f.write("</ul>")
        if removed:
            f.write("<h3 style='color:red;'>➖ Removed Fields:</h3><ul>")
            for field in removed:
                f.write(f"<li>{field}</li>")
            f.write("</ul>")
        if changed_types:
            f.write("<h3 style='color:orange;'>🔁 Changed Types:</h3><ul>")
            for field in changed_types:
                f.write(f"<li>{field}: {old_fields[field]} ➡️ {new_fields[field]}</li>")
            f.write("</ul>")

        if not (added or removed or changed_types):
            f.write("<p>No schema changes detected.</p>")

        f.write("</body></html>")

    print(f"📄 HTML diff report saved: {report_path}")
    webbrowser.open(report_path)

# EMITTER 
emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS_SERVER)

# PROCESS FILES
for file_info in AVRO_FILES_INFO:
    avro_path = file_info["file_path"]
    dataset_name = file_info["dataset_name"]
    dataset_urn = make_dataset_urn(platform=PLATFORM, name=dataset_name, env=ENV)

    print(f"\n🚀 Processing: {dataset_name}")

    dataset_properties = DatasetPropertiesClass(
        description=f"Dataset '{dataset_name}' ingested from AVRO file.",
        customProperties={},
    )

    schema_fields = {}
    with open(avro_path, "rb") as fo:
        avro_reader = reader(fo)
        avro_schema = avro_reader.schema

        for field in avro_schema["fields"]:
            name = field["name"]
            dtype = field["type"]

            # Handle unions like ["null", "string"]
            if isinstance(dtype, list):
                dtype = [d for d in dtype if d != "null"][0]

            native_type = dtype
            if dtype == "string":
                data_type = StringTypeClass()
            elif dtype == "int" or dtype == "long":
                data_type = NumberTypeClass()
            elif dtype == "float" or dtype == "double":
                data_type = NumberTypeClass()
            elif dtype == "boolean":
                data_type = BooleanTypeClass()
            else:
                data_type = StringTypeClass()

            schema_fields[name] = native_type

    # CHECK FOR SCHEMA CHANGE 
    cache_file = f"{SCHEMA_CACHE_DIR}/{dataset_name}.json"
    schema_changed = False
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            old_schema_fields = json.load(f)

        if old_schema_fields != schema_fields:
            schema_changed = True
            print(f"⚠️ Schema change detected for: {dataset_name}")
            generate_html_diff_report(dataset_name, old_schema_fields, schema_fields)
        else:
            print("✅ No schema change.")
    else:
        print("🆕 First time ingest, creating schema cache.")

    with open(cache_file, "w") as f:
        json.dump(schema_fields, f, indent=2)

    # CREATE SCHEMA METADATA 
    schema_field_objs = []
    for field, dtype in schema_fields.items():
        if dtype == "string":
            data_type = StringTypeClass()
        elif dtype in ["int", "long", "float", "double"]:
            data_type = NumberTypeClass()
        elif dtype == "boolean":
            data_type = BooleanTypeClass()
        else:
            data_type = StringTypeClass()

        schema_field_objs.append(
            SchemaFieldClass(
                fieldPath=field,
                type=SchemaFieldDataTypeClass(type=data_type),
                nativeDataType=dtype,
                description=f"Field from AVRO: {field}",
                nullable=True,
            )
        )

    timestamp = int(time.time() * 1000)
    audit_stamp = AuditStampClass(time=timestamp, actor="urn:li:corpuser:unknown")

    schema_metadata = SchemaMetadataClass(
        schemaName=dataset_name,
        platform=f"urn:li:dataPlatform:{PLATFORM}",
        version=0,
        created=audit_stamp,
        lastModified=audit_stamp,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=str(avro_schema)),
        fields=schema_field_objs,
        dataset=dataset_urn,
        cluster=ENV,
    )

    snapshot = DatasetSnapshotClass(
        urn=dataset_urn,
        aspects=[dataset_properties, schema_metadata],
    )

    mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
    emitter.emit(mce)

    print(f"✅ Ingested dataset: {dataset_name}")
