package Test::Tarantool;

use 5.006;
use strict;
use warnings;
use IO::Handle qw/autoflush/;
use Scalar::Util 'weaken';
use AnyEvent::Handle;
use Data::Dumper;

=head1 NAME

Test::Tarantool - The Swiss army knife for testing of Tarantool related Perl code.

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';
our $Count;
our %Schedule;

=head1 SYNOPSIS

    use Test::Tarantool;

    my @shards = map {
        my $shardTest::Tarantool->new(
            spaces => do { open my $f, '<', 'spaces.conf'; local $/ = undef; return <$f>},
            initlua => do { open my $f, '<', 'init.lua'; local $/ = undef; return <$f>},
        );
    } 1..4

    @cluster = map { [ $_->{host}, $_->{p_port} ] } @shards;

    $_->start for (@shards);

    $shards[0]->ro();

    # Some test case here

    $shards[1]->pause();

    # Some test case here

    $shards[1]->resume();
    $shards[0]->rw();

    # stop tarantools and clear work directoies
    @shards = ();

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 SUBROUTINES/METHODS

=head2 new

=cut

sub new {
	my $class = shift; $class = (ref $class)? ref $class : $class;
	$Count++;
	my $self = {
		title => "yat" . $Count,
		root => join("", ("tnt_", map { chr(97 + int(rand(26))) } 1..10)),
		arena => 0.1,
		port => 6603 + 4 * $Count, # FIXME: need feeting
		host => '127.0.0.1',
		wal_mode => 'none',
		log_level => 5,
		initlua => '-- init.lua --',
		replication_source => '',
		logger => sub { warn delete $_[0]->{rbuf} },
		on_die => sub { warn "Broken pipe, child is dead?"; },
		@_,
	};
	$self->{p_port} ||= $self->{port};
	$self->{s_port} ||= $self->{port} + 1;
	$self->{a_port} ||= $self->{port} + 2;
	$self->{r_port} ||= $self->{port} + 3;

	bless $self, $class;

	weaken ($Schedule{$self} = $self);

	mkdir($self->{root}); # FIXME: need error hadling

	$self->_config();
	$self->_init_storage();
	$self->_initlua();
	$self;
}

=head2 start

=cut

sub start {
	my $self = shift;

	pipe my $cr, my $pw or die "pipe filed: $!";
	pipe my $pr, my $cw or die "pipe filed: $!";
	autoflush($_) for ($pr, $pw, $cr, $cw);

	defined(my $pid = fork) or die "Can't fork: $!";
	if ($pid) {
		close($_) for ($cr, $cw);
		$self->{pid} = $pid;
		$self->{rpipe} = $pr;
		$self->{wpipe} = $pw;
		$self->{rh} = AnyEvent::Handle->new(
			fh => $pr,
			on_read => $self->{logger},
			on_error => sub { $self->{pid} = $self->{replication_source} = $self->{sleep} = undef; $self->{on_die}->($self, @_); },
		);
	} else {
		close($_) for ($pr, $pw);
		open(STDIN, "<&", $cr) or die "Could not dup filehandle: $!";
		open(STDOUT, ">&", $cw) or die "Could not dup filehandle: $!";
		open(STDERR, ">&", $cw) or die "Could not dup filehandle: $!";
		print "$self->{root}/tarantool.conf";
		exec "tarantool_box -v -c '$self->{root}/tarantool.conf'";
		die "exec: $!";
	}
}

=head2 stop

=cut

sub stop {
	my $self = shift;
	$self->resume() if delete $self->{sleep};
	$self->_kill('TERM');
	delete $self->{pid}
}

=head2 pause

=cut

sub pause {
	my $self = shift;
	$self->{sleep} = $self->_kill('STOP') unless $self->{sleep}
}

=head2 resume

=cut

sub resume {
	my $self = shift;
	$self->_kill('CONT') if delete $self->{sleep};
}

=head2 ro

=cut

sub ro {
	my ($self, $cb) = @_;
	return if $self->{replication_source};
	$self->{replication_source} = "$self->{host}:$self->{port}";
	$self->_config();
	$self->admin_cmd("reload configuration", sub {
		if (ref $cb eq 'CODE') {
			$cb->($@)
		} else {
			warn "$self->{title}: reload configuration => " . ($_[0] ? "OK" : "Failed")
		}
	});
}

=head2 rw

=cut

sub rw {
	my ($self, $cb) = @_;
	return unless $self->{replication_source};
	$self->{replication_source} = "";
	$self->_config();
	$self->admin_cmd("reload configuration", sub {
		if (ref $cb eq 'CODE') {
			$cb->($@)
		} else {
			warn "$self->{title}: reload configuration => " . ($_[0] ? "OK" : "Failed")
		}
	});
}

=head2 admin_cmd

=cut

sub admin_cmd {
	my ($self, $cmd, $cb) = @_;
	return if ($self->{afh});
	$self->{afh} = AnyEvent::Handle->new (
		connect => [ $self->{host}, $self->{a_port} ],
		on_connect => sub {
			$_[0]->push_write($cmd . "\n");
		},
		on_connect_error => sub {
			warn "Connection error: $_[1]";
			$_[0]->on_read(undef);
			$_[0]->destroy();
			delete $self->{afh};
			$cb->(0, $_[1]);
		},
		on_read => sub {
			my $response = $_[0]->{rbuf};
			$_[0]->on_read(undef);
			$_[0]->destroy();
			delete $self->{afh};
			$cb->(1, $response);
		},
		on_error => sub {
			$_[0]->on_read(undef);
			$_[0]->destroy();
			delete $self->{afh};
			$cb->(0, $_[2])
		},
	);
}

sub _kill {
	my ($self, $signal) = @_;
	return unless ($self->{pid});
	local $_ = kill 0, $self->{pid};
	return $_ unless ($signal and $_);
	kill $signal, $self->{pid} if $signal;
}

sub _config {
	my $self = shift;
	my $config = do { my $pos = tell DATA; local $/; my $c = <DATA>; seek DATA, $pos, 0; $c };
	$config =~ s/ %\{([^{}]+)\} /$self->{$1}/xsg;
	$config =~ s/ %\{\{(.*?)\}\} /eval "$1" or ''/exsg;
	open my $f, '>', $self->{root} . '/' . 'tarantool.conf' or die "Could not create tnt config : $!";;
	syswrite $f, $config;
}

sub _spaces {
	my $self = shift;
	return $self->{spaces} unless ref $self->{spaces};
	die 'TODO';
}

sub _initlua {
	my $self = shift;
	die 'TODO' if ref $self->{initlua};
	open my $f, '>', $self->{root} . '/' . 'init.lua' or die "Could not create init.lua : $!";;
	syswrite $f, $self->{initlua};
}

sub _init_storage() {
	my $self = shift;
	open my $f, '>', $self->{root} . '/' . '00000000000000000001.snap' or die "Could not create tnt snap: $!";
	syswrite $f, "\x53\x4e\x41\x50\x0a\x30\x2e\x31\x31\x0a\x0a\x1e\xab\xad\x10";
}

sub DESTROY {
	my $self = shift;
	return unless $Schedule{$self};
	$self->stop();
	opendir my $root, $self->{root} or die "opendir: $!";
	my @unlink = map { (/^[^.]/ && -f "$self->{root}/$_") ? "$self->{root}/$_" : () } readdir($root);
	local $, = ' ';
	unlink @unlink or
		warn "Could not unlink files (@unlink): $!";
	rmdir($self->{root});
	delete $Schedule{$self};
	warn "$self->{title} destroed\n";
}

END {
	for (keys %Schedule) {
		$Schedule{$_}->DESTROY();
	}
}


=head1 AUTHOR

Anton Reznikov, C<< <a.reznikov at corp.mail.ru> >>

=head1 BUGS

Please report any bugs or feature requests to C<< <a.reznikov at corp.mail.ru> >>



=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Test::Tarantool


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Test-Tarantool>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Test-Tarantool>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Test-Tarantool>

=item * Search CPAN

L<http://search.cpan.org/dist/Test-Tarantool/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2014 Anton Reznikov.

This program is released under the following license: GPL


=cut

1;

__DATA__
custom_proc_title="%{title}"
slab_alloc_arena = %{arena}
bind_ipaddr = %{host}

primary_port = %{p_port}
secondary_port = %{s_port}
admin_port = %{a_port}
replication_port = %{r_port}
%{{ "replication_source = %{replication_source}" if "%{replication_source}" }}

script_dir = %{root}
work_dir = %{root}
wal_mode = %{wal_mode}
log_level = %{log_level}
#logger = "cat - >> tarantool.log"

%{{ $self->_spaces }}
