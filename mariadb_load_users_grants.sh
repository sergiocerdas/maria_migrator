#!/bin/bash
# mariadb_load_users_grants.sh
# Load users and grants into a target MariaDB instance

set -e

# Default values
MYSQL_USER=""
MYSQL_PASSWORD=""
MYSQL_HOST=""
MYSQL_PORT="3306"
GRANTS_FILE=""
USE_SSL="true"

# Usage function
usage() {
	    echo "Usage: $0 -f grants_file -h host -u user -p password [-P port] [--no-ssl]"
	        echo "  -f grants_file         Processed grants SQL file (CREATE USER + GRANT statements)"
		    echo "  -h host                Target MariaDB host (required)"
		        echo "  -u user                Target MariaDB user (required)"
			    echo "  -p password            Target MariaDB password (required)"
			        echo "  -P port                Target MariaDB port (default: 3306)"
				    echo "  --no-ssl               Disable SSL connection (default: SSL enabled)"
				        echo ""
					    echo "Example:"
					        echo "  ./mariadb_load_users_grants.sh -f /backup/grants_processed.sql -h targethost -u admin -p 'password'"
						    exit 1
					    }

					    # Parse arguments
					    while [[ $# -gt 0 ]]; do
						        case $1 in
								        -f)
										            GRANTS_FILE="$2"
											                shift 2
													            ;;
														            -h)
																                MYSQL_HOST="$2"
																		            shift 2
																			                ;;
																					        -P)
																							            MYSQL_PORT="$2"
																								                shift 2
																										            ;;
																											            -u)
																													                MYSQL_USER="$2"
																															            shift 2
																																                ;;
																																		        -p)
																																				            MYSQL_PASSWORD="$2"
																																					                shift 2
																																							            ;;
																																								            --no-ssl)
																																										                USE_SSL="false"
																																												            shift
																																													                ;;
																																															        *)
																																																	            echo "Unknown option: $1"
																																																		                usage
																																																				            ;;
																																																					        esac
																																																					done

																																																					# Validate required parameters
																																																					if [ -z "$GRANTS_FILE" ] || [ -z "$MYSQL_HOST" ] || [ -z "$MYSQL_USER" ] || [ -z "$MYSQL_PASSWORD" ]; then
																																																						    echo "[ERROR] Missing required parameters"
																																																						        usage
																																																					fi

																																																					# Check if grants file exists
																																																					if [ ! -f "$GRANTS_FILE" ]; then
																																																						    echo "[ERROR] Grants file not found: $GRANTS_FILE"
																																																						        exit 1
																																																					fi

																																																					echo "[INFO] Loading users and grants from: $GRANTS_FILE"
																																																					echo "[INFO] Target host: $MYSQL_HOST:$MYSQL_PORT"
																																																					echo "[INFO] Target user: $MYSQL_USER"
																																																					echo "[INFO] SSL enabled: $USE_SSL"

																																																					# Build SSL argument
																																																					SSL_ARG=""
																																																					if [ "$USE_SSL" = "true" ]; then
																																																						    SSL_ARG="--ssl=true"
																																																					fi

																																																					# Load the users and grants
																																																					mariadb --host="$MYSQL_HOST" --port="$MYSQL_PORT" --user="$MYSQL_USER" --password="$MYSQL_PASSWORD" $SSL_ARG < "$GRANTS_FILE"

																																																					echo "[INFO] Users and grants loaded successfully to $MYSQL_HOST:$MYSQL_PORT"

