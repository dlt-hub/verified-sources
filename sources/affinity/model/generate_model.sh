#!/usr/bin/env bash

set -euo pipefail

# TODO: This is currently not fully generating a valid model,
# due to https://github.com/koxudaxi/datamodel-code-generator/pull/2216
datamodel-codegen \
    --input spec.json \
    --output __init__.py \
    --output-model-type pydantic_v2.BaseModel \
    --use-annotated \
    --use-union-operator \
    --capitalise-enum-members \
    --use-field-description \
    --input-file-type openapi \
    --additional-imports pydantic.model_serializer,pydantic.SkipValidation \
    --field-constraints \
    --use-double-quotes
