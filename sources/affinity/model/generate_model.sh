#!/usr/bin/env bash

set -euo pipefail

main() {

    local python_version
    python_version=$(./current_python_major_minor.py)

    # TODO: This is currently not fully generating a valid model,
    # due to https://github.com/koxudaxi/datamodel-code-generator/pull/2216
    datamodel-codegen \
        --input v2_spec.json \
        --output v2.py \
        --output-model-type pydantic_v2.BaseModel \
        --use-annotated \
        --use-union-operator \
        --capitalise-enum-members \
        --use-field-description \
        --input-file-type openapi \
        --field-constraints \
        --use-double-quotes \
        --base-class ..MyBaseModel \
        --disable-timestamp \
        --target-python-version "${python_version}"

    git apply ./v2_model_patches.diff
}

main "$@"