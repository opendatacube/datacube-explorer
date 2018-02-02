
# Deployment Example

Example of running behind nginx on RHEL/CentOS 7.

## Conda

Install conda and the latest stable datacube to location `/opt/conda`

Official docs: [docs](http://datacube-core.readthedocs.io/en/stable/ops/conda.html)

But using environments and cloning datacube manually are probbaly overkill.

Possible alternative:

    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
    bash miniconda.sh -f -b -p /opt/conda
    export PATH="/opt/conda/bin:$PATH"
    
    conda config --prepend channels conda-forge
    conda config --prepend channels conda-forge/label/dev
    conda update --all
    conda install datacube

(the conda-forge datacube will be the latest stable release)

(TODO: conda itself may be overkill on CentOS 7? See how it's 
[installed for Travis](https://github.com/opendatacube/datacube-core/blob/develop/.travis.yml))
## Config

Copy a datacube.conf to `/etc/datacube.conf`

(Only needs to be readable by user running gunicorn/dashboard TODO: system account)

## Dashboard clone

Place dashboard repo in `/var/www/dea-dashboard`

    sudo mkdir /var/www/dea-dashboard
    sudo chown centos /var/www/dea-dashboard
    git clone -b stable \
        https://github.com/data-cube/dea-dashboard.git \
        /var/www/dea-dashboard
    
Install its extra dependencies

    cd /var/www/dea-dashboard
    
    # TODO: conda-specific package list?
    conda install fiona    
    /opt/conda/bin/python ./setup.py develop

## nginx

    yum install epel-release
    yum install nginx lynx goaccess
    
Copy config file. 

(you will need to update the hostname in several places. TODO: Improve)
 
    # Config file
    cp /var/www/dea-dashboard/deployment/deadash.conf \
        /etc/nginx/conf.d/deadash.conf
    
    # Allow nginx to connect to network
    setsebool -P httpd_can_network_connect 1
    
    # Allow nginx to read from /var/www
    chcon -Rt httpd_sys_content_t /var/www

    # Allow through firewall
    iptables -A INPUT -p tcp -m tcp --dport 443 -j ACCEPT
    service iptables save


## SSL/HTTPS

https://certbot.eff.org/#centosrhel7-nginx

# Services

Create user and group `deadash` to run the daemon.

    useradd deadash
    
    # No one else needs read access to config
    chown deadash /etc/datacube.conf
    chmod 600 /etc/datacube.conf
    
(and check that they can read `/var/www/dea-dashboard`).
 
Add as a systemd service

    cp deployment/deadash.service /etc/systemd/system
    chmod 755 /etc/systemd/system/deadash.service  
    systemctl daemon-reload
    
## Kick-off summary generation

TODO: a service for this

    mkdir /var/www/dea-dashboard/product-summaries
    chown deadash /var/www/dea-dashboard/product-summaries
    
    su - deadash
    cd /var/www/dea-dashboard
    nohup /opt/conda/bin/python -m cubedash.generate --all &>> product-summaries/gen.log &


## Start everything

    systemctl enable deadash
    systemctl start  deadash

Status

    systemctl status deadash
    
    # Should have started automatically.
    systemctl status nginx

