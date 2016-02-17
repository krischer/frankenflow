# frankenflow

`frankenflow` is a workflow engine stitching together everything like a mad doctor - intended to ease long running (weeks to months!) iterative workflows. The current realization can orchestrate (an)elastic full seismic waveform inversions with an L-BFGS optimization scheme.

As of now this is not written to be used by anyone but me. Nonetheless feel free to take and utilize what you want and if you have created something valuable: why not give back in terms of a pull request. If there is interest it would also be possible to abstract the concepts a bit more to allow the clean implementation of arbitrary workflows.


### Prerequisites

Sophisticated results require intricate tools and we are standing on the shoulders of a number of great utilities. Thus `frankenflow` requires the installation of:

* Python 3.5 with the following modules
  * `paramiko`
  * `celery`
  * `networkx`
  * `flask`
  * `requests`

Install everything with (Ana)conda:

```bash
$ conda create -n frankenflow python=3.5 paramiko celery networkx flask requests
$ source activate frankenflow
```

Additionally `redis` is required as the message broker. It should be available via most package managers, otherwise just download it and run `make`. 

The current operating mode of `frankenflow` also requires access to a supercomputer to run `SES3D` and quite a lot of disc space. Also `LASIF` and `ses3d_ctrl`/`agere` must be installed (they require a different Python version then `frankenflow` - `conda` environments are a very simple way to set that up).

`frankenflow` is currently tuned to perform a full waveform tomography, but the general concepts translates easily enought to arbitrary workflows. Let me know if there is interest and I'll abstract it a bit more to enable clean implementations of other workflows.

Some more details regarding the utilized conglomerate of tools:

* `LASIF` (http://lasif.net) is used to for the data management and organizational part of the full waveform inversion. Additionally it performs the window selection and adjoint source calculation.
* `SES3D` (MISSING LINK) performs the numerical forward and adjoint simulations. `ses3d_ctrl`/`agere` (MISSING LINK) is responsible for compiling and running SES3D and moving things where they need to go.
* `seismopt` (MISSING LINK) performs and steers the numerical optimization with an L-BFGS algorihtm. 
* `frankenflow` pipes all of these together to form a fully automatic system.



### Getting Started

1. `LASIF` project with a defined Iteration `0`. Thus all sources and receivers and what not needs to be set up. No other iterations should be presetn. Also make sure the window picking is tuned to work well for your tomography.
2. Input files for the forward run `lasif generate_all_input_files 0` copied
   to the HPC to the `hpc_remote_input_files_directory` as specified in the 
   configuration.
3. Initial model in the HDF5 format. Can be created with `agere binary_model_to_hdf5`.
4. A `config.json` akin to the following:

```json
{
	"agere_cmd": "/Users/lion/.miniconda3/envs/obspy_py27/bin/agere",
	"lasif_cmd": "/Users/lion/.miniconda3/envs/obspy_py27/bin/lasif",
	"lasif_project": "/Users/lion/temp/flow_hdf5/LASIF_Project",

	"hpc_remote_host": "localhost",
	"hpc_agere_project": "/Users/lion/temp/yea/SES3D_CTRL_WORKING_DIR",
	"hpc_remote_input_files_directory": "/Users/lion/temp/yea/SES3D_CTRL_WORKING_DIR/input_files",
	"hpc_agere_cmd": "/Users/lion/.miniconda3/envs/obspy_py27/bin/agere",
	"hpc_adjoint_source_folder": "/Users/lion/temp/yea/SES3D_CTRL_WORKING_DIR/adjoint_sources",

	"number_of_events": 2,
	"forward_wavefield_storage_degree": 2,
	"parallel_events": 1,
	"pml_count": 2,
	"walltime_per_event_forward": 0.5,
	"walltime_per_event_adjoint": 0.5
}
```

With the following meaning:

* `agere_cmd`: The full path to the `agere` executable on the local machine.
* `lasif_cmd`: The full path to the `lasif` executable on the local machine.
* `lasif_project`: The full path to the `LASIF` project on the local machine. A lot of disc space should be available.
* `hpc_remote_host`: The name of the HPC host. Use `~/.ssh/config` to set everything up.
* `hpc_agere_project`: The `ses3d_ctrl` working directory on the HPC.
* `hpc_remote_input_files_directory`: A folder with input files for the inversion, already on the HPC.
* `hpc_agere_cmd`:  The full path to the `agere` executable on the HPC machine.
* `hpc_adjoint_source_folder`: The folder where adjoint source are stored on the HPC machine.
* `number_of_events`: The total number of events in the current inversion as a safety mechanism.
* `forward_wavefield_storage_degree`: The degree at which to store the forward wavefield. 2 is a good number.
* `parallel_events`: The number of events that are simulated in parallel.
* `pml_count`: The number of PML layers for the simulations. 3 is a good number.
* `walltime_per_event_forward`: The walltime in hours for the forward simulations per event.
* `walltime_per_event_adjoint`: The walltime in hours for the adjount simulations per event.



```
.
├── __DATA
│   └── 000_model.h5
└── config.json
```

### Running frankenflow

Four things need to run at once. I recommend to use `screen`/`tmux` for this purpose:

1. `redis` - Either run via your system's services or just launch `redis-server`.
2. `celery` workers. Launch with 
   `celery -A frankenflow worker --loglevel=debug --pool=prefork --concurrency=1`
    No need to use a higher concurrency as `frankenflow` is serial for now. I might enable parallel execution in the future as its really easy to do but so far there is really no need for that.
3. The `frankenflow` server. Launch with `python -m frankenflow.server /path/to/flow_folder`.
4. Something to trigger workflow iteration. A simple bash script will do.