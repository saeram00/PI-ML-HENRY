#!/usr/bin/env bash

os=$(uname -o | tr "[:upper:]" "[:lower:]")
project_root_dir=$(pwd)
data_dir="$project_root_dir/data"
app_dir="$project_root_dir/app"
etl_dir="$project_root_dir/ETL"

download_data()
{
    case $1 in
    raw* | pre*)
        fileid="1h9MoA_HQiEggOeBY348hMyD8ZM1zfwaz"
        filename="raw_datasets.tgz"
        ;;
    processed* | post*)
        fileid="1x6caK-XkmvQdPEND7HB6sBBBLMiGli-U"
        filename="processed_datasets.tgz"
        ;;
    esac

    html=$(curl -c ./cookie -sSL "https://drive.google.com/uc?export=download&id=${fileid}")
    curl -Lb ./cookie "https://drive.google.com/uc?export=download&$(echo "${html}" | grep -Po "(confirm=[a-zA-Z0-9\-_]+)")&id=${fileid}" -o ${filename}
    rm ./cookie
    echo "Unpacking datasets..."
    tar -xzf ${filename}
    unset -v {fileid,filename,html}
}

case $1 in
raw* | pre*)
    if ! [ -f "${project_root_dir}/raw_datasets.tgz" ] ; then
        echo "Downloading raw dataset files..."
        download_data $1
    else
        echo "${project_root_dir}/raw_datasets.tgz already exists. Skipping..."
    fi
    if [ "$2" = "etl" ] ; then
        if ! [ -d "${project_root_dir}/.venv" ] ; then
            echo "Setting up python project venv..."
            case $os in
            *msys)
                python -m venv --prompt "venv" .venv
                echo "Starting python project venv..."
                source "${project_root_dir}/.venv/Scripts/activate"
                ;;
            *linux)
                python3 -m venv --prompt "venv" .venv
                echo "Starting python project venv..."
                source "${project_root_dir}/.venv/bin/activate"
                ;;
            esac
        fi
        if [ -z "$(pip3 list | grep -iE "pandas|fastapi|uvicorn")" ] ; then
            echo "Installing required Python libraries..."
            if [ -r "${project_root_dir}/requirements.txt" ] ; then
                pip3 install -U -r "${project_root_dir}/requirements.txt"
            else
                pip3 install -U pandas==1.5.3 fastapi==0.95.0 uvicorn==0.21.1
            fi
        else
            echo "Required Python libraries already installed. Continuing..."
        fi
        echo "Starting etl script..."
        case $os in
        *msys)
            python "${etl_dir}/etl_script.py" && echo "ETL done!!"
            ;;
        *linux)
            python3 "${etl_dir}/etl_script.py" && echo "ETL done!!"
            ;;
        esac
    fi
    echo "Done!!"
    ;;
processed* | post*)
    if ! [ -f "${project_root_dir}/processed_datasets.tgz" ] ; then
        echo "Downloading processed dataset files..."
        download_data $1
    else
        echo "${project_root_dir}/processed_datasets.tgz already exists. Skipping..."
    fi
    echo "Done!!"
    ;;
*)
    echo "Invalid parameter '$1'. First parameter must be one of 'raw', 'pre', 'processed' or 'post'."
    exit 1
    ;;
esac

unset -v {project_root,data,app,etl}_dir os
exit 0
