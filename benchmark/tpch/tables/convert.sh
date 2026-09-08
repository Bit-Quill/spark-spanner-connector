#!/bin/sh

perl -i -pe 's/\|$//' customer.csv
perl -i -pe 's/\|$//' orders.csv
perl -i -pe 's/\|$//' part.csv
perl -i -pe 's/\|$//' partsupp.csv
perl -i -pe 's/\|$//' region.csv
perl -i -pe 's/\|$//' supplier.csv

# Add C_CUSTKEY to LINEITEM using ORDERS.O_ORDERKEY -> ORDERS.O_CUSTKEY
perl -F'\|' -lane '
    BEGIN {
        open(my $fh, "<", "orders.csv") or die "Cannot open orders.csv: $!";
        while (<$fh>) {
            chomp;
            my @fields = split(/\|/, $_, -1);
            $order_to_customer{$fields[0]} = $fields[1];
        }
        close($fh);
    }

    my $order_key = $F[0];
    die "No customer key found for O_ORDERKEY=$order_key\n"
        unless exists $order_to_customer{$order_key};

    print $order_to_customer{$order_key}, "|", join("|", @F);
' lineitem.tbl | perl -pe 's/\|$//' > lineitem.csv
