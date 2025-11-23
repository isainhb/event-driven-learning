FROM php:8.5-apache

# Install dependences (composer, rdkafka)
RUN apt-get update && apt-get install -y librdkafka-dev \
    && pecl install rdkafka \
    && docker-php-ext-enable rdkafka \
    # Install Composer and configure Apache to use Laravel
    && curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer \
    && a2enmod rewrite

# Install dependences (zlib, libzip, unzip, 7z, git)
RUN apt-get install -y zlib1g-dev \
    && apt-get install -y libzip-dev \
    && apt-get install -y unzip \
    && apt-get install -y git

# Install the PHP zip extension
RUN docker-php-ext-install zip

# Set the DocumentRoot to the public directory
ENV APACHE_DOCUMENT_ROOT /var/www/html/public
RUN sed -ri -e 's!/var/www/html!${APACHE_DOCUMENT_ROOT}!g' /etc/apache2/sites-available/*.conf
RUN sed -ri -e 's!/var/www/!${APACHE_DOCUMENT_ROOT}!g' /etc/apache2/apache2.conf /etc/apache2/conf-available/*.conf

WORKDIR /var/www/html

COPY .htaccess /var/www/html/.htaccess
