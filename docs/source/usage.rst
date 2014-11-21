
.. _chapter_usage:

**************************
Usage of the Skeleton Tool
**************************


.. code-block:: shell

    # aimes-skeleton-generate --help

    usage  : aimes-skeleton-generate -i <input> [-o <output>] [-m <mode>] [-v] [-h] [-c] [-r]
    example: aimes-skeleton-generate -i skeleton.in -r -v -m shell

    options:
        -i --input_file, 
            specify input file, ie. the skeleton description (mandatory)
       
        -m --mode
            specify output mode: shell, pegasus, swift, json (default: shell)
       
        -o --output_file
            specify output file (default: - (stdout))
       
        -c --create-setup
            create setup script as <output_file>_setup.sh (default: no)
       
        -r --run_setup
            run setup script (default: no)
       
        -v --verbose
            verbose operation (default: no)
       
        -h --help
            print help (default: no)


    modes:

        shell  : output will be stored a shell script, ie. a set of
                 shell commands which can be executed locally
        json   : output will be stored a json description.
        pegasus: output will be stored a DAX XML description,
                 suitable for the consumption by Pegasus WMS.
        swift  : output will be stored in a swift program,
                 suitable to run under the SWIFT interpreter.

        For all modes, the setup script will be stored as shell
        script (if so requested via '-c').

    .

