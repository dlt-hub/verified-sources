#!/usr/bin/env bash
datamodel-codegen \
    --input *.json \
    --output __init__.py \
    --output-model-type pydantic_v2.BaseModel \
    --use-annotated \
    --use-union-operator \
    --capitalise-enum-members \
    --use-field-description