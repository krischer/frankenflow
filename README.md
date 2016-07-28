# frankenflow

`frankenflow` is a workflow engine stitching together everything like a mad doctor - intended to ease long running (weeks to months!) iterative workflows. **The current realization can orchestrate (an)elastic full seismic waveform inversions using passive seismic data on regional to continental scales with an L-BFGS optimization scheme.** 


### Features

* **Fully Automatic:** Requires no user-interaction after the initial setup and when changing the frequency band.
* **Cell Phone Reporting:** Sends push messages to your cell phone upon important events like reaching a new iteration or launching expensive HPC simulations. Grants a certain degree of control even when away from the PC.
* **Beautiful Web Interface:** Easily get information about the current state, the execution graph, stop and restart it, directly from the browser.
* **Defensive:** Exhaustive run-time checks to assert things go according to plan. Anything strange will cause the workflow to halt and wait for user input. It thus takes care to not waste precious HPC core hours.
* **Informative:** Produces pretty and informative pictures and files so you can always judge the progress of the inversion.


### Disclaimer

As of now **this is not written to be used by anyone but me** and I don't intent to support it. Nonetheless feel free to take and utilize what you want and if you have created something valuable: why not give back in terms of a pull request? Assuming there is interest it would also be possible to abstract the concepts a bit more to allow the clean implementation of arbitrary workflows.


### High-Level Overview

`frankenflow` runs on a local workstation which is used to orchestrate the workflow. It communicates with external compute resources (e.g. your local and trusty big machine) via SSH as that works fine and requires almost no setup.


### Prerequisites

Sophisticated results require intricate tools and we are standing on the shoulders of a number of great utilities. Thus `frankenflow` requires the installation of:

##### Python and Third-Party Modules

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

##### redis

Additionally `redis` is required as the message broker. It should be available via most package managers, otherwise just download it and run `make`. It compiles almost everywhere and has no other dependencies. Great stuff!

##### External Resources

The current operating mode of `frankenflow` also requires access to a supercomputer to run `SES3D` and quite a lot of disc space. Also `LASIF` and `ses3d_ctrl`/`agere` must be installed (they require a different Python version then `frankenflow` - `conda` environments are a very simple way to set that up).

Additionally you must setup key based SSH authentication from the workstation to the HPC. The simple result is that the workstation has to be able to log into the HPC without entering a password. `frankenflow` thus does not have to deal with authentication.


##### Our packages/tools

A couple of other tools are required. `LOCAL` means that it must be installed locally, `HPC` that it must be installed on the HPC.

* `LASIF` (http://lasif.net) - `LOCAL` + `HPC` -  is used to for the data management and organizational part of the full waveform inversion. Additionally it performs the window selection and adjoint source calculation.
* `SES3D` ([link](https://www.ethz.ch/content/specialinterest/erdw/institute-geophysics/computational-seismology/en/software/ses3d.html)) - `HPC` - performs the numerical forward and adjoint simulations. `ses3d_ctrl`/`agere` (https://github.com/krischer/ses3d_ctrl) is responsible for compiling and running SES3D and moving things where they need to go.
* `seismopt` (MISSING LINK - private repository!) - `LOCAL` - performs and steers the numerical optimization with an L-BFGS algorihtm. 
* `frankenflow` - `LOCAL` - pipes all of these together to form a fully automatic system.
* `ses3d_ctrl`/`agere` (https://github.com/krischer/ses3d_ctrl) - `LOCAL` + `HPC` - has to be configured to able to steer SES3D on the HPC.


##### Push Notifications

This is optional but recommended for expensive real world inversions: If you want to receive push notification you will have to sign up to pushover and create a `~/.pushover.json` file with the following contents (fill in your own information of course):

```json
{
    "api_token": "XXX",
    "user_key": "YYY",
    "device": "DEVICE_NAME"
}
```


### Seting up the Inversion

Some of this might seem awkward but that's just how it is right now.

1. `LASIF` project with a defined Iteration `000`. Thus all sources and receivers and what not needs to be set up. No other iterations should be present. Also make sure the window picking is tuned to work well for your tomography.
2. Input files for the forward run, e.g. `lasif generate_all_input_files 000` copied to the HPC to the `hpc_remote_input_files_directory` as specified in the  configuration file.
3. Initial model in the HDF5 format. Can be created with `agere binary_model_to_hdf5`.
4. A `config.json` akin to the following:

```json
{
	"agere_cmd": "/Users/lion/.miniconda3/envs/lasif/bin/agere",
	"lasif_cmd": "/Users/lion/.miniconda3/envs/lasif/bin/lasif",
	"lasif_project": "/Users/lion/temp/flow_hdf5/LASIF_Project",

	"sigma_theta": 0.01,
	"sigma_phi": 0.01,
	"sigma_r": 0.01,

    "max_relative_model_change": 0.05,

	"hpc_remote_host": "kochel",
	"hpc_agere_project": "/export/data/krischer/SES3D_CTRL_CWD",
	"hpc_remote_input_files_directory": "/export/data/krischer/SES3D_CTRL_CWD/input_files",

	"hpc_agere_cmd": "/export/data/krischer/anaconda/envs/obspy_py27/bin/agere",
	"hpc_adjoint_source_folder": "/export/data/krischer/SES3D_CTRL_CWD/adjoint_sources",

	"number_of_events": 2,
	"forward_wavefield_storage_degree": 2,
	"parallel_events": 1,
	"pml_count": 2,
	"walltime_per_event_forward": 0.5,
	"walltime_per_event_adjoint": 0.5,

    "taper_longitude_offset_in_km": 100.0,
    "taper_colatitude_offset_in_km": 100.0,
    "taper_depth_offset_in_km": 100.0,
    "taper_longitude_width_in_km": 100.0,
    "taper_colatitude_width_in_km": 100.0,
    "taper_depth_width_in_km": 100.0
}
```

With the following meaning:

* `agere_cmd`: The full path to the `agere` executable on the local machine.
* `lasif_cmd`: The full path to the `lasif` executable on the local machine.
* `lasif_project`: The full path to the `LASIF` project on the local machine. A lot of disc space should be available.

* `sigma_theta`: Gaussian smoothing sigma in theta direction (colatitude in radian).
* `sigma_phi`: Gaussian smoothing sigma in phi direction (longitude in radian).
* `sigma_r`: Gaussian smoothing sigma in r direction (depth in 1000 km).

* `max_relative_model_change`: The maximum relative change in the model per iteration. Usually only matters for the very first iteration.

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

* `taper_longitude_offset_in_km`: The kernel taper offset in longitude direction.
* `taper_colatitude_offset_in_km`: The kernel taper offset in colatitude direction.
* `taper_depth_offset_in_km`: The kernel taper offset in depth direction.
* `taper_longitude_width_in_km`: The kernel taper width in longitude direction.
* `taper_colatitude_width_in_km`: The kernel taper width in colatitude direction.
* `taper_depth_width_in_km`: The kernel taper width in depth direction.


The initial run directory of the inversion should look thus like this:

```
.
├── __DATA
│   └── 000_model.h5
└── config.json
```

It is probably a fine idea to copy that and also the LASIF project in the initial state as `frankenflow` is unlikely to work the very first try. The copies make it trivial to restart the whole endeavour.


### Running frankenflow

Four things need to run at once. I recommend to use `screen`/`tmux` for this purpose and just launch everything in a different pane/tab:

1. `redis` - Either run via your system's services or just launch `redis-server`.
2. `celery` workers. Launch with 
   `celery -A frankenflow worker --loglevel=debug --pool=prefork --concurrency=1`
    No need to use a higher concurrency as `frankenflow` is serial for now. I might enable parallel execution in the future as its really easy to do but so far there is really no need for that as the workflow is almost completely sequential.
3. The `frankenflow` server. Launch with `python -m frankenflow.server /path/to/flow_folder`.
4. Something to trigger workflow iteration every couple of seconds. A simple bash script will do.


The web interface can be reached at http://localhost:12111

When initially triggered it will 