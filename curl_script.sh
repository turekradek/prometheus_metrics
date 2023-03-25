#!/bin/bash

# Define an array of endpoints
ENDPOINTS=(
                "https://ingest-statistics-prod-monitoring.apps.devops.advantagedp.org/data_pipeline"
                "https://ingest-statistics-prod-monitoring.apps.devops.advantagedp.org/ingest"
                "https://ingest-statistics-prod-monitoring.apps.devops.advantagedp.org/signal_extractor"
                "https://ingest-statistics-prod-monitoring.apps.devops.advantagedp.org/signal_synchronizer"
                "https://ingest-statistics-prod-monitoring.apps.devops.advantagedp.org/endurance_run"
                "https://ingest-statistics-prod-monitoring.apps.devops.advantagedp.org/data_catalog"
                "https://ingest-statistics-prod-monitoring.apps.devops.advantagedp.org/marker_import_tablet_label"
                "https://ingest-statistics-prod-monitoring.apps.devops.advantagedp.org/auto_labelling"
                "https://ingest-statistics-prod-monitoring.apps.devops.advantagedp.org/video_preview"
                "https://ingest-statistics-prod-monitoring.apps.devops.advantagedp.org/image_extractor"
                "https://ingest-statistics-prod-monitoring.apps.devops.advantagedp.org/video_creator"
                "https://ingest-statistics-prod-monitoring.apps.devops.advantagedp.org/dq_repro_rules"
                "https://ingest-statistics-prod-monitoring.apps.devops.advantagedp.org/metrics"
)
endpoints=(
                "http://localhost:8080/data_pipeline"
                "http://localhost:8080/ingest"
                "http://localhost:8080/signal_extractor"
                "http://localhost:8080/signal_synchronizer"
                "http://localhost:8080/endurance_run"
                "http://localhost:8080/data_catalog"
                "http://localhost:8080/marker_import_tablet_label"
                "http://localhost:8080/auto_labelling"
                "http://localhost:8080/video_preview"
                "http://localhost:8080/image_extractor"
                "http://localhost:8080/video_creator"
                "http://localhost:8080/dq_repro_rules"
                "http://localhost:8080/metrics"
)
# Loop through the endpoints and run curl commands
for endpoint in "${endpoints[@]}"
do
    curl "$endpoint" -o "$(basename "$endpoint").txt"
done
