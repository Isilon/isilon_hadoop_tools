# Isilon Hadoop Tools

Tools for Using Hadoop with OneFS

- `isilon_create_users` creates identities needed by Hadoop distributions compatible with OneFS.
- `isilon_create_directories` creates a directory structure with appropriate ownership and permissions in HDFS on OneFS.

![IHT Demo](https://github.com/Isilon/isilon_hadoop_tools/raw/master/demo.gif)

## Installation

Isilon Hadoop Tools (IHT) currently requires Python 3.5+ and supports OneFS 8+.

- Python support schedules can be found [in the Python Developer's Guide](https://devguide.python.org/#status-of-python-branches).
- OneFS support schedules can be found in the [Isilon Product Availability Guide](https://support.emc.com/docu45445_Isilon-Product-Availability.pdf).

### Option 1: Install as a stand-alone command line tool.

<details open>
<summary>Use <code>pipx</code> to install IHT.</summary>
<br>

> _`pipx` requires Python 3.6 or later. For other versions or **offline installations**, see Option 2._

1. [Install `pipx`:](https://pipxproject.github.io/pipx/installation/)

   ``` sh
   python3 -m pip install --user pipx
   ```

   - Tip: Newer versions of some Linux distributions (e.g. [Debian 10](https://packages.debian.org/buster/pipx), [Ubuntu 19.04](https://packages.ubuntu.com/disco/pipx), etc.) offer native packages for `pipx`.

   <br>

   ``` sh
   python3 -m pipx ensurepath
   ```

   - Note: You may need to restart your terminal for the `$PATH` updates to take effect.

2. Use `pipx` to install [`isilon_hadoop_tools`](https://pypi.org/project/isilon_hadoop_tools/):

   ``` sh
   pipx install isilon_hadoop_tools
   ```

3. Test the installation:

   ``` sh
   isilon_create_users --help
   isilon_create_directories --help
   ```

- Use `pipx` to uninstall at any time:

   ``` sh
   pipx uninstall isilon_hadoop_tools
   ```

See Python's [Installing stand alone command line tools](https://packaging.python.org/guides/installing-stand-alone-command-line-tools/) guide for more information.
</details>

### Option 2: Create an ephemeral installation.

<details>
<summary>Use <code>pip</code> to install IHT in a virtual environment.</summary>
<br>

> Python "Virtual Environments" allow Python packages to be installed in an isolated location for a particular application, rather than being installed globally.

1. Use the built-in [`venv`](https://docs.python.org/3/library/venv.html) module to create a virtual environment:

   ``` sh
   python3 -m venv ./iht
   ```

2. Install [`isilon_hadoop_tools`](https://pypi.org/project/isilon_hadoop_tools/) into the virtual environment:

   ``` sh
   iht/bin/pip install isilon_hadoop_tools
   ```

   - Note: This requires access to an up-to-date Python Package Index (PyPI, usually https://pypi.org/).
     For offline installations, necessary resources can be downloaded to a USB flash drive which can be used instead:

      ``` sh
      pip3 download --dest /media/usb/iht-dists isilon_hadoop_tools
      ```
      ``` sh
      iht/bin/pip install --no-index --find-links /media/usb/iht-dists isilon_hadoop_tools
      ```

3. Test the installation:

   ``` sh
   iht/bin/isilon_create_users --help
   ```

   - Tip: Some users find it more convenient to "activate" the virtual environment (which prepends the virtual environment's `bin/` to `$PATH`):

      ``` sh
      source iht/bin/activate
      isilon_create_users --help
      isilon_create_directories --help
      deactivate
      ```

- Remove the virtual environment to uninstall at any time:

   ``` sh
   rm --recursive iht/
   ```

See Python's [Installing Packages](https://packaging.python.org/tutorials/installing-packages/) tutorial for more information.
</details>

## Usage

- Tip: `--help` can be used with any IHT script to see extended usage information.

To use IHT, you will need the following:

- `$onefs`, an IP address, hostname, or SmartConnect name associated with the OneFS System zone
  - Unfortunately, Zone-specific Role-Based Access Control (ZRBAC) is not fully supported by OneFS's RESTful Access to Namespace (RAN) service yet, which is required by `isilon_create_directories`.
- `$iht_user`, a OneFS System zone user with the following privileges:
  - `ISI_PRIV_LOGIN_PAPI`
  - `ISI_PRIV_AUTH`
  - `ISI_PRIV_HDFS`
  - `ISI_PRIV_IFS_BACKUP` (only needed by `isilon_create_directories`)
  - `ISI_PRIV_IFS_RESTORE` (only needed by `isilon_create_directories`)
- `$zone`, the name of the access zone on OneFS that will host HDFS
  - The System zone should **NOT** be used for HDFS.
- `$dist`, the distribution of Hadoop that will be deployed with OneFS (e.g. CDH, HDP, etc.)
- `$cluster_name`, the name of the Hadoop cluster

### Connecting to OneFS via HTTPS

OneFS ships with a self-signed SSL/TLS certificate by default, and such a certificate will not be verifiable by any well-known certificate authority. If you encounter `CERTIFICATE_VERIFY_FAILED` errors while using IHT, it may be because OneFS is still using the default certificate. To remedy the issue, consider encouraging your OneFS administrator to install a verifiable certificate instead. Alternatively, you may choose to skip certificate verification by using the `--no-verify` option, but do so at your own risk!

### Preparing OneFS for Hadoop Deployment

_Note: This is not meant to be a complete guide to setting up Hadoop with OneFS. If you stumbled upon this page or have not otherwise consulted the appropriate install guide for your distribution, please do so at https://community.emc.com/docs/DOC-61379._

There are 2 tools in IHT that are meant to assist with the setup of OneFS as HDFS for a Hadoop cluster:
1. `isilon_create_users`, which creates users and groups that must exist on all hosts in the Hadoop cluster, including OneFS
2. `isilon_create_directories`, which sets the correct ownership and permissions on directories in HDFS on OneFS

These tools must be used _in order_ since a user/group must exist before it can own a directory.

#### `isilon_create_users`

Using the information from above, an invocation of `isilon_create_users` could look like this:
``` sh
isilon_create_users --dry \
    --onefs-user "$iht_user" \
    --zone "$zone" \
    --dist "$dist" \
    --append-cluster-name "$cluster_name" \
    "$onefs"
```
- Note: `--dry` causes the script to log without executing. Use it to ensure the script will do what you intend before actually doing it.

If anything goes wrong (e.g. the script stopped because you forgot to give `$iht_user` the `ISI_PRIV_HDFS` privilege), you can safely rerun with the same options. IHT should figure out that some of its job has been done already and work with what it finds.
- If a particular user/group already exists with a particular UID/GID, the ID it already has will be used.
- If a particular UID/GID is already in use by another user/group, IHT will try again with a different, higher ID.
- IHT may **NOT** detect previous runs that used different options.

##### Generated Shell Script

After running `isilon_create_users`, you will find a new file in `$PWD` named like so:
``` sh
$unix_timestamp-$zone-$dist-$cluster_name.sh
```

This script should be copied to and run on all the other hosts in the Hadoop cluster (excluding OneFS).
It will create the same users/groups with the same UIDs/GIDs and memberships as on OneFS using LSB utilities such as `groupadd`, `useradd`, and `usermod`.

#### `isilon_create_directories`

Using the information from above, an invocation of `isilon_create_directories` could look like this:
``` sh
isilon_create_directories --dry \
    --onefs-user "$iht_user" \
    --zone "$zone" \
    --dist "$dist" \
    --append-cluster-name "$cluster_name" \
    "$onefs"
```
- Note: `--dry` causes the script to log without executing. Use it to ensure the script will do what you intend before actually doing it.

If anything goes wrong (e.g. the script stopped because you forgot to run `isilon_create_users` first), you can safely rerun with the same options. IHT should figure out that some of its job has been done already and work with what it finds.
- If a particular directory already exists but does not have the correct ownership or permissions, IHT will correct it.
- If a user/group has been deleted and re-created with a new UID/GID, IHT will adjust ownership accordingly.
- IHT may **NOT** detect previous runs that used different options.

## Development

See the [Contributing Guidelines](https://github.com/Isilon/isilon_hadoop_tools/blob/master/CONTRIBUTING.md) for information on project development.
