#!/usr/bin/env bash

set -euo pipefail

datamodel-codegen \
    --input *.json \
    --output __init__.py \
    --output-model-type pydantic_v2.BaseModel \
    --use-annotated \
    --use-union-operator \
    --capitalise-enum-members \
    --use-field-description \
    --input-file-type openapi \
    --additional-imports pydantic.model_serializer,pydantic.SkipValidation \
    --field-constraints
