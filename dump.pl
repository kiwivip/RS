#! /usr/bin/env perl

use strict;
use Expect;


my $db_host = "";
my $db_port = "";
my $db_name = "";
my $db_user = "";
my $db_pass = "";
my $backup_file_name = "" ;

my $cmd = "pg_dump --host $db_host --port $db_port --dbname $db_name --username $db_user --file $backup_file_name --format p --password";

my $exp = Expect->new;

$exp->raw_pty(1);

$exp->spawn($cmd);

$exp->expect(
    undef, 'Password:' => sub
    {
        $exp->send($db_pass);
        $exp->send("\n");
        exp_continue;
    }
);
