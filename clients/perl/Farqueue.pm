#!/usr/bin/perl 

package Farqueue;

use strict;
use warnings;

use JSON;
use LWP::UserAgent;
use Time::HiRes;
use Digest::MD5 qw/md5_hex/;
use Sys::Hostname qw/hostname/;

use Data::Dumper qw/Dumper/;

sub new {
    my ($package, $queuename, %args) = @_;

    my $host = $args{Host}||'127.0.0.1';
    my $port = $args{Port}||9094;
    my $requeue = $args{Requeue}||0;

    my $url = "http://$host:$port/$queuename";

    my $json = JSON->new();
    $json->allow_nonref(1);

    my $ua = LWP::UserAgent->new(
        keep_alive => 1,
    );

    return bless {
        ua => $ua,
        url => $url,
        json => $json,
	requeue => $requeue,
    }, $package;
}

sub enqueue {
    my ($self, $data) = @_;

    my $json = $self->{json};
    my $ua = $self->{ua};
    my $url = $self->{url};

    $ua->post($url, Content => $json->encode($data));
}

sub dequeue {
    my ($self, $callback) = @_;

    my $json = $self->{json};
    my $ua = $self->{ua};

    while (1) {
	my $url = $self->{url};

        my $res = $ua->get($url);

        if ($res->code() == 200) {
            my $data = $json->decode($res->content());

            eval {
		$callback->($data);
	    };

	    if ($@) {
		$self->enqueue($data) if $self->{requeue};
		die $@;
	    }

        } elsif ($res->code() == 404) {
            # queue is empty
            sleep 1;
        } else {
            die "Fatal error: " . $res->status_line();
        }
    }
}

sub subscribe {
    my ($self, $callback) = @_;

    $self->dequeue(sub {
	my ($message) = @_;

	my $data = $message->{data};
	my $return_id = $message->{id};

	my $res = $callback->($data);

	my $url = $self->{url};
	
	$self->{url} = $url . $return_id;
	$self->enqueue($res);
	$self->{url} = $url;
    });
}

sub message {
    my ($self, $data, $timeout) = @_;

    $timeout ||= 10;

    my $id = md5_hex(hostname() . Time::HiRes::time() . $$);

    my $message = {
	data => $data,
	id => $id,
    };

    my $url = $self->{url};
    $self->enqueue($message);

    my $result;

    eval {
	local $SIG{ALRM} = sub { die "alarm\n" }; # NB: \n required

	alarm $timeout;

	$self->{url} = $url . $id;
	$self->dequeue(sub {
	    my ($data) = @_;

	    $result = $data;

	    # We have a result, exit here
	    goto END;
	});
    };

END:
    $self->{url} = $url;

    if ($@) {
	die unless $@ eq "alarm\n";
    }

    return $result;
}

1;
