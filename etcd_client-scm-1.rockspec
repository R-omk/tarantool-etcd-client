package = 'etcd_client'
version = 'scm-1'

source  = {
    url    = 'git://github.com/R-omk/tarantool-etcd-client.git';
    branch = 'master';
}

description = {
    summary  = "Etcd 3 client for Tarantool";
    maintainer = "R-omk <romkart@gmail.com>";
    license  = 'BSD2';
}

dependencies = {
    'lua == 5.1';
}

build = {
    type = 'builtin';
    modules = {
        ['etcd_client'] = 'src/init.lua';
    }
}

