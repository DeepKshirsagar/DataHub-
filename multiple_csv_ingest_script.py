import csv
import time
import os
import json
import webbrowser
from datetime import datetime

from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    DatasetPropertiesClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    NumberTypeClass,
    AuditStampClass,
    OtherSchemaClass,
)
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.rest_emitter import DatahubRestEmitter

# ---------------- CONFIG ----------------
DATAHUB_GMS_SERVER = "http://localhost:8080"
PLATFORM = "csv"
ENV = "DEV"

# Resolve absolute paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_CACHE_DIR = os.path.join(BASE_DIR, "schema_cache")
REPORT_DIR = os.path.join(BASE_DIR, "reports")

CSV_FILES_INFO = [
    {"file_path": os.path.join(BASE_DIR, "organizations-100.csv"), "dataset_name": "organizations-100"},
    {"file_path": os.path.join(BASE_DIR, "people-100.csv"), "dataset_name": "people-100"},
    {"file_path": os.path.join(BASE_DIR, "customers-2000000.csv"), "dataset_name": "customers-2000000"},
    {"file_path": os.path.join(BASE_DIR, "customers-100.csv"), "dataset_name": "customers-100"},
    {"file_path": os.path.join(BASE_DIR, "customers-500000.csv"), "dataset_name": "customers-500000.csv"},
    {"file_path": os.path.join(BASE_DIR, "people-500000.csv"), "dataset_name": "people-500000"},
]

# ---------------- CREATE DIRS ----------------
os.makedirs(SCHEMA_CACHE_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

# ---------------- HTML DIFF REPORT ----------------
def generate_html_diff_report(dataset_name, old_fields, new_fields):
    report_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"schema_diff_{dataset_name}_{report_time}.html"
    report_path = os.path.join(REPORT_DIR, filename)

    added = [f for f in new_fields if f not in old_fields]
    removed = [f for f in old_fields if f not in new_fields]

    with open(report_path, "w") as f:
        f.write("<html><head><title>Schema Diff Report</title></head><body>")
        f.write(f"<h2>Schema Diff Report for <code>{dataset_name}</code></h2>")

        if added:
            f.write("<h3 style='color:green;'>➕ Added Fields:</h3><ul>")
            for field in added:
                f.write(f"<li>{field}</li>")
            f.write("</ul>")
        if removed:
            f.write("<h3 style='color:red;'>➖ Removed Fields:</h3><ul>")
            for field in removed:
                f.write(f"<li>{field}</li>")
            f.write("</ul>")

        if not added and not removed:
            f.write("<p>No schema changes detected.</p>")

        f.write("</body></html>")

    print(f"📄 HTML diff report saved: {report_path}")
    webbrowser.open(f"file://{report_path}")

# ---------------- EMITTER ----------------
emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS_SERVER)

# ---------------- PROCESS FILES ----------------
for file_info in CSV_FILES_INFO:
    csv_path = file_info["file_path"]
    dataset_name = file_info["dataset_name"]
    dataset_urn = make_dataset_urn(platform=PLATFORM, name=dataset_name, env=ENV)

    print(f"\n🚀 Processing: {dataset_name}")

    dataset_properties = DatasetPropertiesClass(
        description=f"Dataset '{dataset_name}' ingested from CSV file.",
        customProperties={},
    )

    schema_fields = []

    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        fieldnames = reader.fieldnames

        if not fieldnames:
            raise ValueError(f"CSV '{dataset_name}' has no headers.")

        sample_row = next(reader)

        for field in fieldnames:
            value = sample_row.get(field, "")
            if value.isdigit():
                native_type = "int"
                data_type = NumberTypeClass()
            else:
                native_type = "string"
                data_type = StringTypeClass()

            schema_fields.append(
                SchemaFieldClass(
                    fieldPath=field,
                    type=SchemaFieldDataTypeClass(type=data_type),
                    nativeDataType=native_type,
                    description=f"Field from CSV: {field}",
                    nullable=True,
                )
            )

    # ---------------- TIMESTAMP ----------------
    timestamp = int(time.time() * 1000)
    audit_stamp = AuditStampClass(time=timestamp, actor="urn:li:corpuser:unknown")

    # ---------------- SCHEMA METADATA ----------------
    schema_metadata = SchemaMetadataClass(
        schemaName=dataset_name,
        platform=f"urn:li:dataPlatform:{PLATFORM}",
        version=0,
        created=audit_stamp,
        lastModified=audit_stamp,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=schema_fields,
        dataset=dataset_urn,
        cluster=ENV,
    )

    # ---------------- CHECK FOR SCHEMA CHANGE ----------------
    cache_file = os.path.join(SCHEMA_CACHE_DIR, f"{dataset_name}.json")
    new_field_names = sorted([field.fieldPath for field in schema_fields])

    schema_changed = False
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            old_field_names = json.load(f)
        if old_field_names != new_field_names:
            schema_changed = True
            print(f"⚠️ Schema change detected for: {dataset_name}")
            generate_html_diff_report(dataset_name, old_field_names, new_field_names)
        else:
            print("✅ No schema change.")
    else:
        print("🆕 First time ingest, creating schema cache.")

    with open(cache_file, "w") as f:
        json.dump(new_field_names, f, indent=2)

    # ---------------- EMIT TO DATAHUB ----------------
    snapshot = DatasetSnapshotClass(
        urn=dataset_urn,
        aspects=[dataset_properties, schema_metadata],
    )

    mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
    emitter.emit(mce)

    print(f"✅ Ingested dataset: {dataset_name}")
